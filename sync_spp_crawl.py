from mysql.connector import connect, Error as mysql_error
from mysql.connector import IntegrityError
from dingtalkchatbot.chatbot import DingtalkChatbot
from datetime import datetime, timedelta
from re import sub
from functools import wraps
from traceback import format_exc
import logging
import json
import requests
import urllib.parse
import copy
import time
import re
from env import *

logger = logging.getLogger(__name__)

airport_format = re.compile(r"^[a-zA-Z]{3}$")

airport_cache = {}


class RouteType:
    pick_up = "接机"
    drop_off = "送机"


class PlaceType:
    airport = "机场"
    hotel = "酒店"


def sql_handler(db_pattern="normal"):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            db_pattern_list = db_pattern.split(",")

            cnx = None
            cursor = None
            if "normal" in db_pattern_list:
                cnx = connect(**get_db_env())
                cnx.autocommit = True
                cursor = cnx.cursor(buffered=True)

            if "report" in db_pattern_list:
                r_cnx = connect(**get_report_db_env())
                r_cnx.autocommit = True
                r_cursor = r_cnx.cursor(buffered=True)
                kwargs["r_cnx"] = r_cnx
                kwargs["r_cursor"] = r_cursor

            resultJson = {"restStatus": 200, "body": {}}
            try:
                result = func(*args, cnx=cnx, cursor=cursor, **kwargs)
                return result
            except mysql_error as error:
                logger.error("Sql execution failed:")
                if cnx:
                    logger.error(format_exc())
                    logger.error("SQL execute error: {}".format(error))
                else:
                    logger.error("Database connect error: {}".format(error))
                    logger.error("please check database config.")
                resultJson["restStatus"] = 500
                resultJson["body"] = {
                    "errCode": 10004,
                    "errMsg": "Whoops, something went wrong.",
                }
                return resultJson
            except Exception as error:
                logger.error(format_exc())
                logger.error("Other error: {}".format(error))
                if str(args).find("/v2/search-results") > -1:
                    resultJson["restStatus"] = 500
                    resultJson["body"] = {
                        "errorCode": 10005,
                        "errorMessage": "Whoops, something went wrong.",
                    }
                    return resultJson
                resultJson["restStatus"] = 500
                resultJson["body"] = {
                    "errCode": 10005,
                    "errMsg": "Whoops, something went wrong.",
                }
                return resultJson
            finally:
                if cnx and cnx.is_connected():
                    if cursor:
                        cursor.close()

                    cnx.close()
                    logger.info("Ride MySQL connection is closed")

                if (
                    "r_cnx" in kwargs
                    and kwargs["r_cnx"]
                    and kwargs["r_cnx"].is_connected()
                ):
                    if "r_cursor" in kwargs and kwargs["r_cursor"]:
                        kwargs["r_cursor"].close()

                    kwargs["r_cnx"].close()
                    logger.info("Report MySQL connection is closed")

                if (
                    "a_cnx" in kwargs
                    and kwargs["a_cnx"]
                    and kwargs["a_cnx"].is_connected()
                ):
                    if "r_cursor" in kwargs and kwargs["a_cursor"]:
                        kwargs["a_cursor"].close()

                    kwargs["a_cnx"].close()
                    logger.info("allocate mysql connection is closed")

        return wrapper

    return decorator


def run_sql(cnx, cursor, query, param, multi=False, fetch="all", transaction=False):
    if param is None:
        param = []
    if isinstance(param, list):
        param = tuple(param)
    elif not isinstance(param, tuple):
        param = (param,)
    display_query = query
    print(sub("\s+", " ", sub("\n", "\t", display_query)))
    print("Param: ", end="")
    print(param)
    is_start_transaction = False
    if transaction and not cnx.in_transaction:
        is_start_transaction = True
        cnx.start_transaction()
    try:
        if multi:
            result = cursor.execute(query, param, multi=True)
            return result
        else:
            cursor.execute(query, param)
            if fetch in ("all", "one"):
                f = getattr(cursor, "fetch" + fetch)
                result = f()
                return result
            else:
                return cursor.rowcount
    finally:
        if is_start_transaction:
            cnx.commit()


def get_query_condition_parameter(info, operator):
    param = []
    condition_list = []
    operator = operator or {}

    for key, value in info.items():
        if type(value) in (tuple, list):
            param.extend(value)
            if key in operator:
                if operator[key] == "like":
                    condition_list.append(
                        "({})".format(" or ".join([f"{key} like %s"] * len(value)))
                    )
                else:
                    condition_list.append(
                        "{} {} ({})".format(
                            key, operator[key], ",".join(["%s"] * len(value))
                        )
                    )
            else:
                condition_list.append(
                    "{} in ({})".format(key, ",".join(["%s"] * len(value)))
                )
        else:
            param.append(value)
            if key in operator:
                condition_list.append("{} {} %s".format(key, operator[key]))
            else:
                condition_list.append(f"{key}=%s")
    if "connector" in operator:
        condition = f' {operator["connector"]} '.join(condition_list)
    else:
        condition = " and ".join(condition_list)

    return condition, param


def get_keys_placeholder_and_param(info, kargs=None):
    kargs = kargs or {}

    if "inner_variables" in kargs:
        inner_variables = kargs["inner_variables"]
    else:
        inner_variables = []
    if isinstance(info, list):
        keys = ",".join(info[0].keys())
        param = []
        place_list = []
        for item in info:
            one_param = []
            pl_list = []
            for key in item:
                if key not in inner_variables:
                    one_param.append(item[key])
                    pl_list.append("%s")
                else:
                    pl_list.append(item[key])

            param.extend(one_param)
            pl = ",".join(pl_list)
            pl = f"({pl})"
            place_list.append(pl)

        placeholder = ",".join(place_list)

        return keys, placeholder, param
    else:
        keys = ",".join(info.keys())
        param = []
        place_list = []

        for key in info:
            if key not in inner_variables:
                param.append(info[key])
                place_list.append("%s")
            else:
                place_list.append(info[key])

        placeholder = ",".join(place_list)
        placeholder = f"({placeholder})"

        return keys, placeholder, param


def get_last_id_in_spp_crawl(cnx, cursor):
    query = """select scl.id
    from spp_crawl_route scl
    order by id desc
    limit 1
    ;"""
    param = []
    result = run_sql(cnx, cursor, query, param, fetch="one")

    if result is None:
        return None
    else:
        return dict(zip(cursor.column_names, result))["id"]


def spp_cost_select_by_info(info, cnx, cursor, operator={}):
    query = """select srt.id as spp_route_id,
    srt.name as route_name, srt.from_place, srt.to_place,
    srt.from_place_lat_lng, srt.to_place_lat_lng,
    srt.from_address, srt.to_address,
    srt.platform_name as platform_name, srt.partner_id as partner_id,
    srt.service_area_id_elife as service_area_id_elife,
    srt.json as route_json, srt.is_active, srt.batch,
    srt.tz,srt.crawl_state,
    srt.disable_date,
    srt.tz
    from spp_route srt
    where {}
    {}
    order by srt.id asc
    ;"""

    if len(info) == 0:
        return None

    condition, param = get_query_condition_parameter(info, operator)

    spp_crawl_condition = ""
    # last_spp_crawl_id  = get_last_id_in_spp_crawl(cnx, cursor)
    # if last_spp_crawl_id:
    #     spp_crawl_condition = f"and srt.id > {last_spp_crawl_id}"

    # spp_route_last_updated_at = (datetime.now() - timedelta(hours=8 + 24)).strftime('%Y-%m-%d %H:%M:%S')
    spp_route_last_updated_at = (datetime.now() - timedelta(hours=8 + 10)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    # spp_crawl_condition = f" and srt.last_updated_at >= %s"
    # param.append(spp_route_last_updated_at)

    # spp_crawl_condition = " and srt.partner_id = %s "
    # param.append(2621)

    query = query.format(condition, spp_crawl_condition)
    result = run_sql(cnx, cursor, query, param, fetch="all")

    if result is None:
        return []
    else:
        return [dict(zip(cursor.column_names, row)) for row in result]


@sql_handler(db_pattern="report")
def insert_spp_crawl(spp_crawl_info, r_cnx, r_cursor, **kwargs):
    query = """insert into
            spp_crawl_route({})
            values{} 
            on duplicate key update {};"""

    keys, placeholder, param = get_keys_placeholder_and_param(spp_crawl_info)

    update_info = copy.deepcopy(spp_crawl_info)
    update_info.pop("id")
    update_fields = [
        # "route_name",
        # "partner_id",
        # "platform_name",
        # "service_area_id",
        "disable_date",
        # "start_place_name_manual",
        # "start_place_lat",
        # "start_place_lng",
        # "end_place_name_manual",
        # "end_place_lat",
        # "end_place_lng",
        # "ctrip_flight_no",
        # "remark",
        # "active",
        # "route_zone_str",
        # "batch",
        # "route_type",
    ]

    update_info_keys_list = list(update_info.keys())
    for key in update_info_keys_list:
        if key not in update_fields:
            update_info.pop(key)
    update_line_list = []
    for k, v in update_info.items():
        update_line_list.append(f"{k}=%s")
        param.append(v)
    update_info_line = ",".join(update_line_list)

    query = query.format(keys, placeholder, update_info_line)

    try:
        row_no = run_sql(r_cnx, r_cursor, query, param, fetch="no")
    except IntegrityError as error:
        if error.msg.find("Duplicate entry") > -1:
            logger.warning("note_info record already exists")
            return 0
        else:
            logger.error(format_exc())
            raise

    if row_no > 0:
        return r_cursor.lastrowid
    else:
        return -1


def select_airport(airport_info, cnx, cursor, operator={}):
    query = """select code3, name, google_place_id
    from airport
    where {}
    limit 1
    ;"""

    if len(airport_info) == 0:
        return None

    condition, param = get_query_condition_parameter(airport_info, operator)

    query = query.format(condition)
    result = run_sql(cnx, cursor, query, param, fetch="one")

    if result is None:
        return None
    else:
        return dict(zip(cursor.column_names, result))


def select_flight_no(airport_info, cnx, cursor, operator={}, is_filtered_by_time=True):
    query = """select f.from_airport, f.to_airport, 
    f.code as flight_code, f.flight_number
    from flight f
    left join flight_sch fs on f.flight_sch_id = fs.id
    where {}
    {}
    and not f.flight_sch_id is null
    ;"""

    if len(airport_info) == 0:
        return None

    condition, param = get_query_condition_parameter(airport_info, operator)

    if is_filtered_by_time is True:
        filter_by_time = "and fs.to_hh >= 8 and fs.to_hh <= 18"
    else:
        filter_by_time = ""

    query = query.format(condition, filter_by_time)
    results = run_sql(cnx, cursor, query, param, fetch="all")

    if results is None:
        return None
    else:
        return [dict(zip(cursor.column_names, result)) for result in results]


def filter_different_flight_no(flight_no_list: list) -> list:
    filtered_diffenct_flight_no_list = list()

    from_airport_flight_no_set = set()
    for flight_no_data in flight_no_list:
        from_airport = flight_no_data.get("from_airport")
        if from_airport not in from_airport_flight_no_set:
            filtered_diffenct_flight_no_list.append(flight_no_data)
            from_airport_flight_no_set.add(from_airport)

    return filtered_diffenct_flight_no_list


def get_google_place_id_from_place(place):
    url = "https://maps.googleapis.com/maps/api/geocode/json?"

    param = {"address": place, "key": "AIzaSyD9mzj4sfSNhPUOmfp605SpbSMmip6TtD4"}
    try:
        res = requests.get(f"{url}{urllib.parse.urlencode(param)}")
        if res.status_code == 200:
            result_json = res.json()
            return result_json["body"]["result"][0]["place_id"]
        else:
            return None
    except:
        return None


def read_ssp_route_data(cnx, cursor):
    # spp_cost_info = {1: 1}  # 更新一段时间内全部路线
    spp_cost_info = {
        "srt.id": [
         3248

          
       #  1280,1281,1282,1283,1284,1285,1286,1287,1288,1289,1290,1291,1293,1295,1297,1298,1299,1300,1301,1302,1303,1304,1305,1306,1307,1308,1309,1310,1311,1312,1313,1314,1315,1316,1317,1318,1319,1320,1321,1322,1323,1324,1325,1326,1327,1328,1329,1330,1331,1332,1333,1334,1335,1338,1340,1341,1343,1344,1345,1347,1349,1350,1351,1353,1355,1357,1359,1360,1361,1362,1363,1364,1365,1366,1367,1368,1369,1370,1371,1372,1373,1374,1375,1376,1378,1379,1380,1381,1382,1383,1384,1385,1386,1387,1388,1389,1390,1391,1392,1393,1394,1395,1396,1397,1398,1399,1400,1401,1402,1403,1404,1405,1406,1407,1408,1409,1410,1411,1412,1413,1414,1415,1416,1417,1418,1419,1420,1421,1422,1423,1424,1425,1426,1427,1428,1429,1430,1431,1432,1433,1434,1435,1436,1437,1438,1439,1440,1441,1442,1443,1444,1445,1446,1447,1448,1449,1450,1451,1452,1453,1454,1455,1456,1457,1458,1459,1460,1461,1462,1463,1464,1465,1466,1467,1468,1469,1470,1471,1472,1473,1474,1475,1476,1477,1478,1479,1480,1481,1482,1483,1484,1485,1486,1487,1488,1489,1490,1491,1492,1493,1494,1495,1498,1499,1500,1501,1502,1503,1504,1505,1506,1507,1508,1509,1510,1511,1512,1513,1514,1515,1516,1517,1518,1519,1520,1521,1522,1523,1524,1525,1526,1527,1528,1529,1530,1531,1532,1533,1534,1535,1536,1537,1538,#1539,1540,1541,1542,1543,1544,1545,1546,1547,1548,1549,1550,1551,1552,1553,1554,1555,1556,1557,1558,1559,1560,1561,1562,1563,1564,1565,1566,1567,1568,1569,1570,1571,1572,1573,1574,1575,1576,1577,1578,1579,1580,1581,1582,1583,1584,1585,1586,1587,1588,1589,1590,1591,1592,1593,1594,1595,1596,1597,1598,1599,1600,1601,1602,1603,1604,1605,1606,1607,1608,1609,1610,1611,1612,1613,1614,1615,1616,1617,1618,1619,1620,1621,1622,1623,1624,1625,1626,1627,1632,1633,1634,1635,1636,1637,1638,1639,1640,1641,1642,1643,1644,1645,1647,1648,1649,1650,1651,1652,1653,1654,1655,1656,1657,1658,1659,1660,1661,1662,1663,1664,1665,1666,1667,1668,1669,1670,1671,1672,1677,1678,1679,1680,1681,1682,1683,1684,1685,1686,1687,1688,1689,1690,1691,1692,1693,1694,1695,1696,1697,1698,1699,1700,1701,1702,1703,1704,1705,1706,1707,1708,1709,1710,1715,1716,1717,1718,1719,1720,1721,1722,1723,1724,1725,1726,1728,1729,1730,1731,1732,1733,1734,1735,1736,1737,1738,1739,1740,1741,1742,1743,1744,1745,1746,1747,1748,1749,1750,1751,1752,1754,1758,1759,1760,1761,1762,1763,1764,1765,1766,1767,1768,1769,1770,1771,1772,1773,1774,1775,1776,1777,1778,1779,1780,1781,1782,1783,1784,1785,1786,1787,1788,1789,1790,1791,1792,1793,1794,1795,1796,1797,1798,1799,1800,1801,1802,1803,1804,1805,1806,1807,1808,1809,1810,1811,1812,1813,1814,1815,1816,1817,1818,1819,1821,1827,1828,1829,1830,1831,1832,1833,1834,1835,1836,1837,1838,1839,1840,1841,1842,1843,1844,1845,1846,1847,1848,1849,1850,1851,1852,1853,1854,1855,1856,1857,1858,1859,1860,1861,1862,1863,1864,1865,1866,1867,1869,1871,1873,1875,1877,1879,1881,1882,1883,1884,1885,1886,1887,1888,1889,1890,1891,1892,1893,1894,1895,1896,1897,1898,1899,1900,1901,1902,1903,1904,1906,1908,1909,1910,1911,1912,1913,1914,1915,1916,1917,1918,1919,1920,1921,1922,1923,1924,1925,1926,1927,1928,1929,1930,1931,1932,1933,1934,1935,1936,1937,1938,1939,1940,1941,1942,1943,1944,1945,1946,1947,1948,1949,1950,1951,1952,1953,1954,1955,1956,1957,1958,1959,1960,1961,1962,1963,1964,1965,1966,1967,1968,1969,1970,1971,1972,1973,1974,1975,1976,1977,1978,1979,1980,1981,1982,1983,1984,1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,
        ]
    }  # 更新一段时间指定的路线列表
    # spp_cost_info = {"srt.id": 777} //# 更新一段时间指定的唯一的一条路线
    spp_cost_result = spp_cost_select_by_info(spp_cost_info, cnx, cursor)
    print(spp_cost_result)
    return spp_cost_result


def get_zone_name(from_address, to_address, from_place, to_place, route_type):
    print(from_address, to_address)
    zone_name = None
    if route_type == RouteType.pick_up:
        if len(to_address) != 3 or not airport_format.match(str(to_address)):
            zone_name = to_place
    elif route_type == RouteType.drop_off:
        if len(from_address) != 3 or not airport_format.match(str(from_address)):
            zone_name = from_place
    return zone_name


def get_filtered_route_json(route_json: dict):
    filtered_route_json = copy.deepcopy(route_json)
    if "d_amt" in route_json and "p_amt" in route_json:
        filtered_route_json.pop("p_amt")
    return filtered_route_json


def send_dingtalk_msg(msg):
    webhook = get_dingtalk_webhook()
    bot = DingtalkChatbot(webhook)
    bot.send_text(msg)


@sql_handler()
def process(cnx, cursor):
    warning_spp_route_id = []

    ssp_route_data = read_ssp_route_data(cnx, cursor)
    for item in ssp_route_data:
        insert_spp_crawl_data = {}
        insert_spp_crawl_data["id"] = int(item["spp_route_id"])
        insert_spp_crawl_data["disable_date"] = item["disable_date"]

        if False:
            insert_spp_crawl_data["route_name"] = item["route_name"]
            insert_spp_crawl_data["partner_id"] = item["partner_id"]
            insert_spp_crawl_data["platform_name"] = (
                item["platform_name"] if item["platform_name"] is not None else ""
            )
            insert_spp_crawl_data["service_area_id"] = item["service_area_id_elife"]
    

            if item["is_active"] is not None and int(item["is_active"]) < 0:
                insert_spp_crawl_data["active"] = 0
            else:
                insert_spp_crawl_data["active"] = 1
            if item["crawl_state"] is not None and item["crawl_state"] == 2:
                insert_spp_crawl_data["active"] = 2

            insert_spp_crawl_data["batch"] = item["batch"]
            insert_spp_crawl_data["route_zone_str"] = item["tz"]

            route_json = None
            try:
                route_json = json.loads(item["route_json"])
            except Exception:
                logger.info(format_exc())

            start_place_lat_lnt_data = {}
            try:
                start_place_lat_lnt_data = json.loads(item["from_place_lat_lng"])
            except Exception:
                logger.info(format_exc)

            insert_spp_crawl_data["start_place_lat"] = start_place_lat_lnt_data.get("lat")
            insert_spp_crawl_data["start_place_lng"] = start_place_lat_lnt_data.get("lng")

            insert_spp_crawl_data["start_place_name_manual"] = item["from_address"]
            insert_spp_crawl_data["end_place_name_manual"] = item["to_address"]

            print(item["to_address"])

            end_place_lat_lnt_data = {}
            try:
                end_place_lat_lnt_data = json.loads(item["to_place_lat_lng"])
            except Exception:
                logger.info(format_exc())
            insert_spp_crawl_data["end_place_lat"] = end_place_lat_lnt_data["lat"]
            insert_spp_crawl_data["end_place_lng"] = end_place_lat_lnt_data["lng"]

            route_type = None
            from_place = item["from_place"]
            to_place = item["to_place"]
            from_address = item["from_address"]
            to_address = item["to_address"]
            if len(from_place) == 3:
                airport_code = airport_cache.get(from_place, "")
                if not airport_code:
                    airport_code = select_airport({"code3": from_place}, cnx, cursor)
                    airport_cache[from_place] = airport_code

                if airport_code:
                    route_type = RouteType.pick_up
                    insert_spp_crawl_data["start_place_google_place_id"] = airport_code[
                        "google_place_id"
                    ]
                    insert_spp_crawl_data["start_place_type"] = PlaceType.airport

                    insert_spp_crawl_data["start_place_name_manual"] = airport_code["code3"]
            if not insert_spp_crawl_data.get("start_place_type"):
                if item["from_address"] is not None:
                    if (
                        "酒店" in item["from_address"]
                        or "hotel" in item["from_address"].lower()
                    ):
                        insert_spp_crawl_data["start_place_type"] = PlaceType.hotel

            if len(to_place) == 3:
                airport_code = airport_cache.get(to_place, "")
                if not airport_code:
                    airport_code = select_airport({"code3": to_place}, cnx, cursor)
                    airport_cache[to_place] = airport_code

                if airport_code:
                    route_type = RouteType.drop_off
                    insert_spp_crawl_data["end_place_google_place_id"] = airport_code[
                        "google_place_id"
                    ]
                    insert_spp_crawl_data["end_place_type"] = PlaceType.airport

                    print(airport_code["code3"])
                    insert_spp_crawl_data["end_place_name_manual"] = airport_code["code3"]
            if not insert_spp_crawl_data.get("end_place_type"):
                if item["to_address"] is not None:
                    if (
                        "酒店" in item["to_address"]
                        or "hotel" in item["to_address"].lower()
                    ):
                        insert_spp_crawl_data["end_place_type"] = PlaceType.hotel

            insert_spp_crawl_data["route_type"] = route_type

            if route_type == RouteType.pick_up:
                remark = {}
                flight_infos = select_flight_no({"f.to_airport": from_place}, cnx, cursor)
                flight_infos = filter_different_flight_no(flight_no_list=flight_infos)
                if flight_infos:
                    if item["partner_id"] and int(item["partner_id"]) == 2621:
                        flight_info = flight_infos[0]
                        insert_spp_crawl_data["ctrip_flight_no"] = (
                            f"{flight_info['flight_code']}{flight_info['flight_number']}"
                        )
                        if "from_airport" in flight_info:
                            remark["from_airport"] = flight_info.get("from_airport")
                        if "to_airport" in flight_info:
                            remark["to_airport"] = flight_info.get("to_airport")
                        # 加上备选航班
                        ctrip_flight_list = [
                            {
                                "from_airport": i.get("from_airport"),
                                "to_airport": i.get("to_airport"),
                                "flight_no": f"{i['flight_code']}{i['flight_number']}",
                            }
                            for i in flight_infos[1:11]
                        ]
                        remark.update(
                            {
                                "from_airport": flight_info.get("from_airport"),
                                "to_airport": flight_info.get("to_airport"),
                                "ctrip_flight_list": ctrip_flight_list,
                            }
                        )
                else:
                    if item["partner_id"] and int(item["partner_id"]) == 2621:
                        flight_infos = select_flight_no(
                            {"f.to_airport": from_place},
                            cnx,
                            cursor,
                            is_filtered_by_time=False,
                        )
                        if flight_infos:
                            flight_info = flight_infos[0]
                            insert_spp_crawl_data["ctrip_flight_no"] = (
                                f"{flight_info['flight_code']}{flight_info['flight_number']}"
                            )
                            if "from_airport" in flight_info:
                                remark["from_airport"] = flight_info.get("from_airport")
                            if "to_airport" in flight_info:
                                remark["to_airport"] = flight_info.get("to_airport")
                            # 加上备选航班
                            ctrip_flight_list = [
                                {
                                    "from_airport": i.get("from_airport"),
                                    "to_airport": i.get("to_airport"),
                                    "flight_no": f"{i['flight_code']}{i['flight_number']}",
                                }
                                for i in flight_infos[1:11]
                            ]
                            remark.update(
                                {
                                    "from_airport": flight_info.get("from_airport"),
                                    "to_airport": flight_info.get("to_airport"),
                                    "ctrip_flight_list": ctrip_flight_list,
                                }
                            )

                zone_name = get_zone_name(
                    from_address=from_address,
                    to_address=to_address,
                    from_place=from_place,
                    to_place=to_place,
                    route_type=route_type,
                )
                if zone_name:
                    remark["zone_name"] = zone_name
                insert_spp_crawl_data["remark"] = json.dumps(remark, ensure_ascii=False)

            elif route_type == RouteType.drop_off:
                remark = {}
                # flight_info = select_flight_no({"f.to_airport": to_place}, cnx, cursor)
                # if flight_info:
                #     insert_spp_crawl_data['ctrip_flight_no'] = f"{flight_info['flight_code']}{flight_info['flight_number']}"
                #     if item['partner_id'] and int(item['partner_id']) == 2621:
                #         insert_spp_crawl_data['ctrip_flight_no'] = f"{flight_info['flight_code']}{flight_info['flight_number']}"
                #         # remark.update({"from_airport": flight_info.get('from_airport'), "to_airport": flight_info.get("to_airport")})
                #         if "from_airport" in flight_info:
                #             remark['from_airport'] = flight_info.get('from_airport')
                #         if "to_airport" in flight_info:
                #             remark['to_airport'] = flight_info.get('to_airport')
                zone_name = get_zone_name(
                    from_address=from_address,
                    to_address=to_address,
                    from_place=from_place,
                    to_place=to_place,
                    route_type=route_type,
                )
                if zone_name:
                    remark["zone_name"] = zone_name
                insert_spp_crawl_data["remark"] = json.dumps(remark, ensure_ascii=False)

            # 判定spp_route的接送机类型是否和spp_crawl_spp的接送机类型是否一致
            route_json = get_filtered_route_json(route_json=route_json)
            if not insert_spp_crawl_data.get("route_type"):
                if (
                    insert_spp_crawl_data.get("route_type") == RouteType.drop_off
                    and "d_amt" not in route_json
                ):
                    warning_spp_route_id.append(item["route_id"])
                    continue
                if (
                    insert_spp_crawl_data.get("route_type") == RouteType.pick_up
                    and "p_amt" not in route_json
                ):
                    warning_spp_route_id.append(item["route_id"])
                    continue

        print(insert_spp_crawl_data)

        insert_spp_crawl(insert_spp_crawl_data)

    if warning_spp_route_id:
        send_dingtalk_msg(
            msg=f"写spp_crawl_route表发现spp_route中异常数据: {warning_spp_route_id}"
        )


def main():
    # while 1:
    #     process()
    #     send_dingtalk_msg(msg="写spp_crawl_route成功！")
    #     time.sleep(60 * 60 * 0.5)
    process()


if __name__ == "__main__":
    main()
