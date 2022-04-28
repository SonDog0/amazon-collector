from connection.mariadb import connection_177_ktspi

import sys
import requests
import time
import glob
import json
import pandas as pd
import traceback

from pathlib import Path
from datetime import datetime

now = datetime.now()

review_home = "/mnt/storage/data/result-amazon-review"

# config
with open("config/config.json") as f:
    config = json.load(f)


def collector_review(item, asin_list, today_datetime):

    """
    Required
        asin : Product ASIN

    Optional
        page : Page numberg
        country : Marketplace country(default US)
        filter_by_star : Filter reviews by stars. Empty value will return all reviews
            '' - return all reviews(default)
            1 - return only 1 star reviews
            2 - return only 2 star reviews
            3 - return only 3 star reviews
            4 - return only 4 star reviews
            5 - return only 5 star reviews
        variants : If a product has multiple product variants, then by default you will receive reviews related to all
                   product variants. If you need reviews only for specified ASIN then set it to 0
                    1 - reviews for all product variants (default)
                    0 - reviews only for specified product ASIN
        top : By default reviews will be sorted by \\"Most Recent\\" reviews, if you need to sort them by \\"Top Reviews\\"
              then set this value to 1

    """

    conn, cur = connection_177_ktspi()
    sql_get_max_date = "select max(date_unix) from amazon_review"
    cur.execute(sql_get_max_date)
    max_date = int(cur.fetchall()[0][0])
    print(max_date)

    Path(review_home + f"/{item}/{today_datetime}").mkdir(parents=True, exist_ok=True)
    result_path = review_home + f"/{item}/review-all/{today_datetime}.csv"

    for asin in asin_list:

        page = 1
        df = pd.DataFrame()

        try:
            while True:

                url = "https://amazon-product-reviews-keywords.p.rapidapi.com/product/reviews"

                querystring = {"asin": asin, "country": "US", "page": page}

                headers = {
                    "x-rapidapi-key": config["rapid_id"],
                    "x-rapidapi-host": "amazon-product-reviews-keywords.p.rapidapi.com",
                }

                response = requests.get(url, params=querystring, headers=headers)

                raw = json.loads(response.text)

                data = pd.json_normalize(raw["reviews"])

                for i in range(0, len(data)):
                    if data.iloc[i]["date.unix"] <= max_date:
                        select_index = i - 1
                        if select_index < 0:
                            data = pd.DataFrame()
                            print("break for unix time")
                            print("at first row")
                            df = df.append(data)
                            raise Exception

                        else:
                            data = data.iloc[:i]
                            print("break for unix time")
                            df = df.append(data)
                            raise Exception

                next_page = raw["next_page"]
                reviews = raw["reviews"]

                df = df.append(data)

                if next_page == -1 or not next_page or next_page is None or not reviews:
                    print(f"stop collection at page {next_page}")
                    break

                page += 1

                print(raw)

                print(data)

        except Exception as e:
            print(traceback.print_exc())
            print(e)

        finally:
            df.drop_duplicates("id", inplace=True)
            df.to_csv(
                review_home + f"/{item}/{today_datetime}/{asin}.csv",
                encoding="utf-8-sig",
                index=False,
            )

    time.sleep(5)

    appended_data = []
    cnt = 0
    for infile in glob.glob(review_home + f"/{item}/{today_datetime}/*.csv"):
        cnt += 1
        try:
            print("enter for")
            data = pd.read_csv(infile, encoding="utf-8-sig")
            print(data)
            # store DataFrame in list
            print(infile, cnt)
            # print(appended_data)
            appended_data.append(data)
        except:
            continue

    # see pd.concat documentation for more info
    appended_data = pd.concat(appended_data)

    appended_data.to_csv(result_path, index=False, encoding="utf-8-sig")

    return result_path


def sql_amazon_review_csv_insert(csv_path, today):

    conn, cur = connection_177_ktspi()

    sql_amazon_review_by_csv = """
    LOAD DATA LOCAL INFILE '{}' 
                INTO TABLE amazon_review_temp
                FIELDS TERMINATED BY ','
                ESCAPED BY ''
                ENCLOSED BY '"'                
                LINES TERMINATED BY '\n'
                IGNORE 1 ROWS
                (@id, @review_data, @name, @rating, @title, @review, @verified_purchase, @asin.original, @asin.variant, @date.date, @date.unix, @dates)
                SET	id = @id,
                	rating = @rating,
                	title = @title,
                	review = @review,
                	verified_purchase = @verified_purchase,
                	asin = @asin.original,
                	date = FROM_UNIXTIME(@date.unix),
                	date_unix = @date.unix,
                	created_at = '{} 00:00:00'
    """.format(
        csv_path, today
    )

    cur.execute(sql_amazon_review_by_csv)

    conn.commit()
