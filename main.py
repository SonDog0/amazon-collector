import sys

from amazonCollector.search import (
    collector_search,
    get_asin_csv,
    collector_listing,
    join_listring_search,
    sql_amazon_csv_insert,
)
from amazonCollector.review import collector_review, sql_amazon_review_csv_insert
from common.utils import set_logger
from datetime import datetime

now = datetime.now()
today = now.strftime("%Y-%m-%d")
today_datetime = now.strftime("%Y%m%d%H%M%S")[2:]

## Logging
logger = set_logger("main-{}".format(today))
logger.info(f"main start. today : {today} , datetime : {today_datetime}")


## SEARCH

collector_search(item="cosmetic", today=today)

asin_list = get_asin_csv(item="cosmetic", today=today, today_datetime=today_datetime)

collector_listing(item="cosmetic", asin_list=asin_list, today_datetime=today_datetime)

join_listring_search("cosmetic", today_datetime)

search_result = join_listring_search("cosmetic", today_datetime)

sql_amazon_csv_insert(search_result, today)


## REVIEW

review_result = collector_review(
    item="cosmetic", today_datetime=today_datetime, asin_list=asin_list
)

sql_amazon_review_csv_insert(review_result, today)
