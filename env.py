def get_db_env():
    # return {
    #     'user': 'dev_user',
    #     'password': '0TAnwLmMzDbCYMuf',
    #     'host': 'rideprod.c9y2b7qgnkr8.us-east-2.rds.amazonaws.com',
    #     'database': 'ride'
    # } # dev     
    return {
        'user': 'prod_RO_hehao',
        'password': 'Sm9Knq3qz7O7EA96gMZZ',
        'host': 'rideprod.clasi6jcghkh.us-east-2.rds.amazonaws.com',
        'database': 'ride'
    } # prod

def get_report_db_env():
    # return {
    #     'user': 'dev_user',
    #     'password': '0TAnwLmMzDbCYMuf',
    #     'host': 'rideprod.c9y2b7qgnkr8.us-east-2.rds.amazonaws.com',
    #     'database': 'ride'
    # } # dev
     return {
        'user': 'prod_rpt_crawl',
        'password': '6v0wjyH8BO8Lr9g2u0mlL',
        'host': 'report.clasi6jcghkh.us-east-2.rds.amazonaws.com',
        'database': 'report'
    } # prod

def get_dingtalk_webhook():
    return "https://oapi.dingtalk.com/robot/send?access_token=79c8f053360b109c99c74e49a9e2ca5583cfa446ec85f7610cb7e03b7e113ff6"