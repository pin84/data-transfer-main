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
    and not f.flight_sch_id is None
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

# on duplicate key update {}


def insert_spp_crawl(item, cnx, cursor):
    query = """insert into spp_crawl_route ({spp_crawl_route_columns})
    values ({spp_crawl_route_placeholders})
    on duplicate key update {update_info_line}
    ;"""
    spp_crawl_route_columns = ','.join(item.keys())
    spp_crawl_route_placeholders = ','.join(['%s'] * len(item.keys()))

    update_info = copy.deepcopy(item)
    update_info.pop("id")

    param = list(item.values())

    update_line_list = []
    for k, v in update_info.items():
        update_line_list.append(f"{k}=%s")
        param.append(v)
    update_info_line = ",".join(update_line_list)
    query = query.format(
        spp_crawl_route_columns=spp_crawl_route_columns,
        spp_crawl_route_placeholders=spp_crawl_route_placeholders,
        update_info_line=update_info_line
    )
    result = run_sql(cnx, cursor, query, param, fetch="no")
    print("result", result)
    return result


@sql_handler(db_pattern="report")
def read_ssp_route_data_proc(ids, r_cnx, r_cursor, **kwargs):
    query = f"""select id,
       partner_id,
       platform_name,
       service_area_id,
       start_place_name_manual,
       start_place_lat,
       start_place_lng,
       start_place_google_place_id,
       start_place_address,
       start_place_display_name,
       start_place_type,
       end_place_name_manual,
       end_place_lat,
       end_place_lng,
       end_place_google_place_id,
       end_place_address,
       end_place_display_name,
       end_place_type,
       ctrip_flight_no,
       route_type,
       remark,
       route_zone_str2,
       route_name,
       active,
       level,
       disable_date
    from spp_crawl_route
    where id in ({",".join([str(i) for i in ids])})
    ;"""
    result = run_sql(r_cnx, r_cursor, query, None, fetch="all")
    if result is None:
        return []
    else:
        return [dict(zip(r_cursor.column_names, row)) for row in result]


@sql_handler()
def process(cnx, cursor):


    ids = [
        2010,2011,2013
    ]

    report_data = read_ssp_route_data_proc(ids)
    for item in report_data:
        ssp_route_data = insert_spp_crawl(item, cnx, cursor)

    print("-----------END-----")


def main():
    # while 1:
    #     process()
    #     send_dingtalk_msg(msg="写spp_crawl_route成功！")
    #     time.sleep(60 * 60 * 0.5)
    process()


if __name__ == "__main__":
    main()
