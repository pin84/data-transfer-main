def get_db_env():
    # return {
    #     'user': 'dev_user',
    #     'password': '0TAnwLmMzDbCYMuf',
    #     'host': 'rideprod.c9y2b7qgnkr8.us-east-2.rds.amazonaws.com',
    #     'database': 'ride'
    # } # dev     
    return {
        'user': 'prod_ro_ping',
        'password': 'B5VD72B53H824l1G7vy0',
        'host': 'rideprod.clasi6jcghkh.us-east-2.rds.amazonaws.com',
        'database': 'ride',
        'connect_timeout': 10,
        'autocommit': True,
        'ssl_disabled': True,
        'auth_plugin': 'mysql_native_password',
        'use_pure': True
    } # prod


  


def get_report_db_env():
    return {
        'user': 'dev_user',
        'password': '0TAnwLmMzDbCYMuf',
        'host': 'rideprod.c9y2b7qgnkr8.us-east-2.rds.amazonaws.com',
        'database': 'ride',
        'connect_timeout': 10,
        'autocommit': True,
        'ssl_disabled': True,
        'auth_plugin': 'mysql_native_password',
        'use_pure': True
    } # dev
    #  return {
    #     'user': 'prod_rpt_crawl',
    #     'password': '6v0wjyH8BO8Lr9g2u0mlL',
    #     'host': 'report.clasi6jcghkh.us-east-2.rds.amazonaws.com',
    #     'database': 'report'
    # } # prod
    # return {
    #     'user': 'dev_user',
    #     'password': '0TAnwLmMzDbCYMuf',
    #     'host': 'rideprod.c9y2b7qgnkr8.us-east-2.rds.amazonaws.com',
    #     'database': 'ride'
    # } 

# def get_dingtalk_webhook():
#     return "https://oapi.dingtalk.com/robot/send?access_token=79c8f053360b109c99c74e49a9e2ca5583cfa446ec85f7610cb7e03b7e113ff6"