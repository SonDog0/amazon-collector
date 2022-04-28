import os
import glob
from keywords.enKeyword import get_cosmetic_keyword, get_cloth_keyword
from common.utils import set_logger
from connection.mariadb import connection_177_ktspi

import pandas as pd
import requests
import json
import traceback
from pathlib import Path
from datetime import datetime
import sys
import logging
import time
from urllib import parse


search_home = "/mnt/storage/data/result-amazon-serach"


# today = now.strftime("%Y-%m-%d")


now = datetime.now()
log_today = now.strftime("%Y-%m-%d")
log_datetime = now.strftime("%Y%m%d%H%M%S")[2:]

# config
with open("/mnt/storage/collection-amazon/config/config.json") as f:
    config = json.load(f)


def collector_search(item, today):

    if item == "cosmetic":
        keywords = get_cosmetic_keyword()
    elif item == "cloth":
        keywords = get_cloth_keyword()
        print(len(keywords))
        sys.exit(0)

    else:
        raise Exception(
            "Error! please check item string. you can use; 1.cosmetic, 2.cloth"
        )

    save_dir_path = f"/mnt/storage/data/result-amazon-serach/{item}/{today}"

    Path(save_dir_path).mkdir(parents=True, exist_ok=True)

    logger = set_logger("collector_search", log_today)

    for keyword in keywords:
        keyword = keyword.replace("/", " ")
        print(keyword)
        save_csv_path = (
            f"/mnt/storage/data/result-amazon-serach/{item}/{today}/{keyword}"
        )
        page = 0
        break_point = 0
        df = pd.DataFrame(
            columns=[
                "asin",
                "full_link",
                "image",
                "currency",
                "current_price",
                "previous_price",
                "prime",
                "stars",
                "total_reviews",
                "sponsored",
                "title",
            ]
        )
        try:
            while True:

                page += 1

                url = "https://amazon-products1.p.rapidapi.com/search"

                querystring = {"country": "US", "query": keyword, "page": page}

                headers = {
                    "x-rapidapi-key": config["rapid_id"],
                    "x-rapidapi-host": "amazon-products1.p.rapidapi.com",
                }

                response = requests.get(
                    url, params=parse.urlencode(querystring), headers=headers
                )

                raw = json.loads(response.text)

                data = {}
                spon_count = 0

                # Step1 : API -> CSV

                partition_raw = len(raw["results"])

                print(
                    f" keyword : {keyword}, nowpage : {page}, partition_raws  : {partition_raw} , length_of_df : {len(df)}"
                )

                for i in range(0, partition_raw):
                    asin = raw["results"][i]["asin"]
                    full_link = raw["results"][i]["full_link"]
                    image = raw["results"][i]["image"]
                    currency = raw["results"][i]["prices"]["currency"]
                    current_price = raw["results"][i]["prices"]["current_price"]
                    previous_price = raw["results"][i]["prices"]["previous_price"]
                    prime = raw["results"][i]["prime"]
                    stars = raw["results"][i]["reviews"]["stars"]
                    total_reviews = raw["results"][i]["reviews"]["total_reviews"]
                    sponsored = raw["results"][i]["sponsored"]
                    title = raw["results"][i]["title"]

                    if sponsored == True:
                        spon_count += 1
                        # print(f'spond_count : {spon_count}')

                    data.update(
                        {
                            "asin": asin,
                            "full_link": full_link,
                            "image": image,
                            "currency": currency,
                            "current_price": current_price,
                            "previous_price": previous_price,
                            "prime": prime,
                            "stars": stars,
                            "total_reviews": total_reviews,
                            "sponsored": sponsored,
                            "title": title,
                        }
                    )
                    df = df.append(data, ignore_index=True)

                if spon_count == partition_raw:
                    # stop when all raw is SPON
                    if partition_raw == 0:
                        pass
                    else:
                        df = df[: int(partition_raw) * -1]

                    break_point += 1
                    print(
                        f""" detect all data is spon, remove raws. 
                         keyword : {keyword} , spon_count : {spon_count},
                         partition_raw : {partition_raw} , break_point : {break_point} """
                    )

                if break_point == 2:
                    break

        except Exception as e:
            logger.exception(
                "==== {}, {} =====".format(keyword, now.strftime("%Y%m%d%H%M%S")[2:])
            )
            print(e)
            print(traceback.print_exc())
            print(len(raw), raw)

        finally:

            df = df[df["sponsored"] != True]

            # make directory
            Path(save_csv_path).mkdir(parents=True, exist_ok=True)
            # save df to csv
            df.to_csv(
                r"{}/rapid_api_amazon_yellow_{}_{}.csv".format(
                    save_csv_path,
                    keyword,
                    today,
                ),
                encoding="utf-8-sig",
                index=False,
            )


def get_asin_csv(item, today, today_datetime):

    dirlist = os.listdir(search_home + f"/{item}/{today}")  # returns list
    print(dirlist)
    cnt = 0
    appended_data = []

    for dir in dirlist:
        for infile in glob.glob(search_home + f"/{item}/{today}/{dir}/*.csv"):
            cnt += 1
            # print(infile , cnt)
            data = pd.read_csv(infile, encoding="utf-8-sig")
            # store DataFrame in list
            # print(appended_data)
            appended_data.append(data)
        # see pd.concat documentation for more info

    appended_data = pd.concat(appended_data)
    appended_data.drop_duplicates("asin", inplace=True)

    Path(search_home + f"/{item}/search-all").mkdir(exist_ok=True)
    appended_data.to_csv(
        search_home + f"/{item}/search-all/{today_datetime}.csv",
        encoding="utf-8-sig",
        index=False,
    )

    return appended_data["asin"].tolist()


def collector_listing(item, asin_list, today_datetime):

    Path(search_home + f"/{item}/listing/{today_datetime}").mkdir(
        parents=True, exist_ok=True
    )

    logger = set_logger("collector_listing", log_today)

    for asin in asin_list:

        page = 1
        df = pd.DataFrame()
        duplicate_stack = 0
        pre_title = "sonunknownrandomstringson"
        asin = asin.replace("/", " ")

        while True:
            try:

                url = "https://amazon-products1.p.rapidapi.com/listing"

                querystring = {
                    "country": "US",
                    "asin": "{}".format(asin),
                    "page": page,
                }

                headers = {
                    "x-rapidapi-key": "91518c4abemsh67574248fcb7791p14e645jsnef4a6b69275c",
                    "x-rapidapi-host": "amazon-products1.p.rapidapi.com",
                }

                response = requests.get(
                    url, params=parse.urlencode(querystring), headers=headers
                )

                raw = json.loads(response.text)

                print(response.text)

                next_page = raw["next_page"]
                page = next_page

                title = raw["title"]

                if pre_title == title:
                    duplicate_stack += 1
                else:
                    pre_title = title

                data = pd.json_normalize(raw["results"])
                data["title"] = title
                df = df.append(data)

                if page == -1 or not page or page is None or duplicate_stack >= 10:
                    print(f"stop collect at page {page}")
                    break

            except Exception as e:
                logger.exception(
                    "==== {}, {} =====".format(asin, now.strftime("%Y%m%d%H%M%S")[2:])
                )
                print(e)
                break
            finally:
                df.to_csv(
                    search_home + f"/{item}/listing/{today_datetime}/{asin}.csv",
                    encoding="utf-8-sig",
                    index=False,
                )
    return 0


def join_listring_search(item, today_datetime):
    result_path = search_home + f"/{item}/join_search_listing/{today_datetime}.csv"
    Path(search_home + f"/{item}/join_search_listing").mkdir(exist_ok=True)

    def get_asin_listing_cnt():

        # path = r"D:\amazon\amazon_search\yellow\listing\{}".format(datetime_path)
        path = search_home + f"/{item}/listing/{today_datetime}"
        csv_files = glob.glob(os.path.join(path, "*.csv"))

        # print(csv_files)

        dicts = {}
        result = pd.DataFrame()

        for csv in csv_files:
            try:
                df = pd.read_csv(csv, encoding="utf-8-sig")
                df.drop("title", axis=1, inplace=True)
                df.drop_duplicates(inplace=True)

                dicts.update(
                    {"asin": os.path.basename(csv)[:-4], "listing_cnt": len(df)}
                )

            except Exception as e:
                dicts.update({"asin": os.path.basename(csv)[:-4], "listing_cnt": 0})
            finally:
                result = result.append(dicts, True)

        print(result)
        return result

    df = pd.read_csv(
        search_home + f"/{item}/search-all/{today_datetime}.csv",
        encoding="utf-8-sig",
    )

    # # listing_cnt about cleansing asin
    listing_cnt = get_asin_listing_cnt()
    #
    # print("listing_done")
    df = df.set_index("asin")
    #
    listing_cnt = listing_cnt.set_index("asin")
    #
    # print("merge start")
    result = pd.merge(
        left=df, right=listing_cnt, left_index=True, right_index=True, how="inner"
    )
    # print("merge done")
    #
    result.to_csv(
        result_path,
        encoding="utf-8-sig",
    )
    print("Finish join_listring_search Function")

    return result_path


def sql_amazon_csv_insert(csv_path, today):
    conn, cur = connection_177_ktspi()
    logger = set_logger("sql_amazon_csv_insert", today)

    sql_insert_amazon_by_csv = """
    LOAD DATA LOCAL INFILE '{}' 
                INTO TABLE amazon_backup
                FIELDS TERMINATED BY ','
                ESCAPED BY ''
                ENCLOSED BY '"'                
                LINES TERMINATED BY '\n'
                IGNORE 1 ROWS
                (@asin, @full_link, @image, @currency, @current_price, @previous_price, @prime, @stars, @total_reviews, @sponsored, @title, @listing_cnt)
                SET	asin = @asin,
                	full_link = @full_link,
                	image = @image,
                	currency = @currency,
                	current_price = @current_price,
                	stars = @stars,
                	total_reviews = @total_reviews,
                	title = @title,
                	listing_cnt = @listing_cnt,
                	created_at = '{} 00:00:00'
    """.format(
        csv_path, today
    )

    try:
        cur.execute(sql_insert_amazon_by_csv)
    except Exception as e:
        print(e)
        logger.exception("SQL ERROR {}-{}".format(e, today))
        return 1
    conn.commit()
