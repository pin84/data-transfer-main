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
                        "({})".format(" or ".join(
                            [f"{key} like %s"] * len(value)))
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


def spp_cost_select_by_info(info, cnx, cursor, operator={}):
    query = """select srt.id as spp_route_id,
    srt.name as route_name, 
    srt.from_place, 
    srt.to_place,
    srt.from_place_lat_lng, 
    srt.to_place_lat_lng,
    srt.from_address, 
    srt.to_address,
    srt.platform_name as platform_name, 
    srt.partner_id as partner_id,
    srt.service_area_id_elife as service_area_id_elife,
    srt.json as route_json, 
    srt.is_active, 
    srt.batch,
    srt.crawl_state,
    srt.disable_date,
    srt.tz_id
    from spp_route_purchasing srt
    where {}
    {}
    order by srt.id asc
    ;"""

    if len(info) == 0:
        return None

    condition, param = get_query_condition_parameter(info, operator)
    spp_crawl_condition = ""
    query = query.format(condition, spp_crawl_condition)
    result = run_sql(cnx, cursor, query, param, fetch="all")

    if result is None:
        return []
    else:
        return [dict(zip(cursor.column_names, row)) for row in result]


@sql_handler(db_pattern="report")
def insert_spp_crawl(spp_crawl_info, r_cnx, r_cursor, **kwargs):
    print(f"Starting insert_spp_crawl for ID: {spp_crawl_info.get('id')}")
    query = """insert into
            spp_crawl_route_purchasing({})
            values{} 
            on duplicate key update {};"""

    keys, placeholder, param = get_keys_placeholder_and_param(spp_crawl_info)
    print(f"Keys: {keys}")
    print(f"Placeholder: {placeholder}")
    print(f"Params count: {len(param)}")

    update_info = copy.deepcopy(spp_crawl_info)
    update_info.pop("id")
    update_fields = [
        "route_name",
        "partner_id",
        "platform_name",
        "service_area_id",
        "disable_date",
        "start_place_name_manual",
        "start_place_lat",
        "start_place_lng",
        "end_place_name_manual",
        "end_place_lat",
        "end_place_lng",
        "ctrip_flight_no",
        "remark",
        "active",
        "route_zone_str",
        "batch",
        "route_type",
        "route_zone_str2"
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
    print(f"Final query: {query}")
    print(f"Final params: {param}")

    try:
        print("Executing SQL...")
        row_no = run_sql(r_cnx, r_cursor, query, param, fetch="no")
        print(f"SQL executed, row_no: {row_no}")
    except IntegrityError as error:
        if hasattr(error, 'msg') and error.msg.find("Duplicate entry") > -1:
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





def read_ssp_route_data(cnx, cursor):
    print('Reading SSP route data...')
    # spp_cost_info = {1: 1}  # 更新一段时间内全部路线
    spp_cost_info = {
        "srt.id": [
          
  7576,7577,7578,7579,7580,7581,7582,7583,7584,7585,7586,7587,7588,7589,7590,7591,7592,7593,7594,7595,7596,7597,7598,7599,7600,7601,7602,7603,7604,7605,7606,7607,7608,7609,7610,7611,7612,7613,7614,7615,7616,7617,7618,7619,7620,7621,7622,7623,7624,7625,7626,7627,7628,7629,7630,7631,7632,7633,7634,7635,7636,7637,7638,7639,7640,7641,7642,7643,7644,7645,7646,7647,7648,7649,7650,7651,7652,7653,7654,7655,7656,7657,7658,7659,7660,7661,7662,7663,7664,7665,7666,7667,7668,7669,7670,7671,7672,7673,7674,7675,7676,7677,7678,7679,7680,7681,7682,7683,7684,7685,7686,7687,7688,7689,7690,7691,7692,7693,7694,7695,7696,7697,7698,7699,7700,7701,7702,7703,7704,7705,7706,7707,7708,7709,7710,7711,7712,7713,7714,7715,7716,7717,7718,7719,7720,7721,7722,7723,7724,7725,7726,7727,7728,7729,7730,7731,7732,7733,7734,7735,7736,7737,7738,7739,7740,7741,7742,7743,7744,7745,7746,7747,7748,7749,7750,7751,7752,7753,7754,7755,7756,7757,7758,7759,7760,7761,7762,7763,7764,7765,7766,7767,7768,7769,7770,7771,7772,7773,7774,7775,7776,7777,7778,7779,7780,7781,7782,7783,7784,7785,7786,7787,7788,7789,7790,7791,7792,7793,7794,7795,7796,7797,7798,7799,7800,7801,7802,7803,7804,7805,7806,7807,7808,7809,7810,7811,7812,7813,7814,7815,7816,7817,7818,7819,7820,7821,7822,7823,7824,7825,7826,7827,7828,7829,7830,7831,7832,7833,7834,7835,7836,7837,7838,7839,7840,7841,7842,7843,7844,7845,7846,7847,7848,7849,7850,7851,7852,7853,7854,7855,7856,7857,7858,7859,7860,7861,7862,7863,7864,7865,7866,7867,7868,7869,7870,7871,7872,7873,7874,7875,7876,7877,7878,7879,7880,7881,7882,7883,7884,7885,7886,7887,7888,7889,7890,7891,7892,7893,7894,7895,7896,7897,7898,7899,7900,7901,7902,7903,7904,7905,7906,7907,7908,7909,7910,7911,7912,7913,7914,7915,7916,7917,7918,7919,7920,7921,7922,7923,7924,7925,7926,7927,7928,7929,7930,7931,7932,7933,7934,7935,7936,7937,7938,7939,7940,7941,7942,7943,7944


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




@sql_handler()
def process(cnx, cursor):
    print('--2222-')
    print('Starting to read SSP route data...')

    ssp_route_data = read_ssp_route_data(cnx, cursor)
    print(f'Read {len(ssp_route_data) if ssp_route_data else 0} route records')

    for item in ssp_route_data:
        insert_spp_crawl_data = {}

        partner_id = item["partner_id"]

        insert_spp_crawl_data["id"] = int(item["spp_route_id"])
        if partner_id != 2:
            insert_spp_crawl_data["route_zone_str2"] = item["tz_id"]

        if item["disable_date"] == 'None':
            insert_spp_crawl_data["disable_date"] = None
        else:
            insert_spp_crawl_data["disable_date"] = item["disable_date"]

        if True:
            insert_spp_crawl_data["route_name"] = item["route_name"]
            insert_spp_crawl_data["partner_id"] = item["partner_id"]
            insert_spp_crawl_data["platform_name"] = (
                item["platform_name"] if item["platform_name"] is not None else ""
            )
            insert_spp_crawl_data["service_area_id"] = item["service_area_id_elife"]

            insert_spp_crawl_data["active"] = 0
            # if item["is_active"] is not None and int(item["is_active"]) <= 0:
            #     insert_spp_crawl_data["active"] = 0
            # else:
            #     insert_spp_crawl_data["active"] = 1
            # if item["crawl_state"] is not None and item["crawl_state"] == 2:
            #     insert_spp_crawl_data["active"] = 2

            insert_spp_crawl_data["batch"] = item["batch"]

            route_json = None
            try:
                route_json = json.loads(item["route_json"])
            except Exception:
                route_json = {}
                logger.info(format_exc())

            start_place_lat_lnt_data = {}
            try:
                start_place_lat_lnt_data = json.loads(
                    item["from_place_lat_lng"])
            except Exception:
                logger.info(format_exc())

            insert_spp_crawl_data["start_place_name_manual"] = item["from_address"]
            insert_spp_crawl_data["end_place_name_manual"] = item["to_address"]
            end_place_lat_lnt_data = {}
            try:
                end_place_lat_lnt_data = json.loads(item["to_place_lat_lng"])
            except Exception:
                logger.info(format_exc())

            if partner_id != 2:
                insert_spp_crawl_data["start_place_lat"] = start_place_lat_lnt_data.get(
                    "lat", '')
                insert_spp_crawl_data["start_place_lng"] = start_place_lat_lnt_data.get(
                    "lng", '')
                insert_spp_crawl_data["end_place_lat"] = end_place_lat_lnt_data.get(
                    'lat', '')
                insert_spp_crawl_data["end_place_lng"] = end_place_lat_lnt_data.get(
                    'lng', '')

            route_type = None
            from_place = item["from_place"]
            to_place = item["to_place"]
            from_address = item["from_address"]
            to_address = item["to_address"]
            if len(from_place) == 3:
                airport_code = airport_cache.get(from_place, "")
                if not airport_code:
                    airport_code = select_airport(
                        {"code3": from_place}, cnx, cursor)
                    airport_cache[from_place] = airport_code

                if airport_code:
                    route_type = RouteType.pick_up
                    insert_spp_crawl_data["start_place_google_place_id"] = airport_code[
                        "google_place_id"
                    ]
                    insert_spp_crawl_data["start_place_type"] = PlaceType.airport

                    # insert_spp_crawl_data["start_place_name_manual"] = airport_code["code3"]
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
                    airport_code = select_airport(
                        {"code3": to_place}, cnx, cursor)
                    airport_cache[to_place] = airport_code

                if airport_code:
                    route_type = RouteType.drop_off
                    insert_spp_crawl_data["end_place_google_place_id"] = airport_code[
                        "google_place_id"
                    ]
                    insert_spp_crawl_data["end_place_type"] = PlaceType.airport
                    # insert_spp_crawl_data["end_place_name_manual"] = airport_code["code3"]
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
                flight_infos = select_flight_no(
                    {"f.to_airport": from_place}, cnx, cursor)
                flight_infos = filter_different_flight_no(
                    flight_no_list=flight_infos)
                if flight_infos:
                    if item["partner_id"] and int(item["partner_id"]) == 2621:
                        flight_info = flight_infos[0]
                        insert_spp_crawl_data["ctrip_flight_no"] = (
                            f"{flight_info['flight_code']}{flight_info['flight_number']}"
                        )
                        if "from_airport" in flight_info:
                            remark["from_airport"] = flight_info.get(
                                "from_airport")
                        if "to_airport" in flight_info:
                            remark["to_airport"] = flight_info.get(
                                "to_airport")
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
                                remark["from_airport"] = flight_info.get(
                                    "from_airport")
                            if "to_airport" in flight_info:
                                remark["to_airport"] = flight_info.get(
                                    "to_airport")
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
                insert_spp_crawl_data["remark"] = json.dumps(
                    remark, ensure_ascii=False)

            elif route_type == RouteType.drop_off:
                remark = {}
                zone_name = get_zone_name(
                    from_address=from_address,
                    to_address=to_address,
                    from_place=from_place,
                    to_place=to_place,
                    route_type=route_type,
                )
                if zone_name:
                    remark["zone_name"] = zone_name
                insert_spp_crawl_data["remark"] = json.dumps(
                    remark, ensure_ascii=False)

            # 判定spp_route的接送机类型是否和spp_crawl_spp的接送机类型是否一致
            route_json = get_filtered_route_json(route_json=route_json)
            if not insert_spp_crawl_data.get("route_type"):
                if (
                    insert_spp_crawl_data.get(
                        "route_type") == RouteType.drop_off
                    and "d_amt" not in route_json
                ):
                    continue
                if (
                    insert_spp_crawl_data.get(
                        "route_type") == RouteType.pick_up
                    and "p_amt" not in route_json
                ):
                    continue
        print(insert_spp_crawl_data)
        insert_spp_crawl(insert_spp_crawl_data)



def main():
    print('--435435--')
    print('About to call process()...')
    try:
        process()
        print('process() completed successfully')
    except Exception as e:
        print(f'Error in process(): {e}')
        import traceback
        traceback.print_exc()
    print('---3-')


if __name__ == "__main__":
    main()
