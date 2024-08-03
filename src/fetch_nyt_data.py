from datetime import datetime, timedelta
import logging
import requests
import os
import pandas as pd
import time
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas


logger = logging.getLogger("NYT_API_BOOKS")
logger.setLevel(logging.INFO)


NYT_API_KEY = os.getenv("NYT_API_KEY")
API_URL = "https://api.nytimes.com/svc/books/v3/lists/overview.json"
DATE_FORMAT = "%Y-%m-%d"

SF_CREDENTIALS = dict(
    account=os.getenv("SF_ACCOUNT"),
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    role=os.getenv("SF_ROLE"),
    warehouse=os.getenv("SF_WAREHOUSE"),
    database=os.getenv("SF_DATABASE"),
    schema=os.getenv("SF_SCHEMA")
)

years = [2023, 2022, 2021]


def fetch_week_lists(date: str, api_key: str, api_url: str = API_URL) -> list:
    request_url = f"{api_url}?published_date={date}&api-key={api_key}"
    request_headers = {"Accept": "application/json"}

    response = requests.get(url=request_url, headers=request_headers)

    if response.status_code == 200:
        return response.json()["results"]["lists"]
    else:
        logger.warning(f"Failed to fetch data from {date}: status: {response.status_code}")
        return []


def extract_boor_uri(dict_list):
    return [d["book_uri"] for d in dict_list]


df_books = []
df_lists = []
for year in years:
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)

    current_date = start_date
    while current_date <= end_date:
        logger.info(f"Fetching data for week of {current_date.strftime(DATE_FORMAT)}")
        week_lists = fetch_week_lists(date=current_date.strftime(DATE_FORMAT), api_key=NYT_API_KEY)
        
        if week_lists:
            logger.info(f"Creating df with list")
            for wl in week_lists:
                df_list = pd.DataFrame(wl)
                df_list = df_list[["list_id", "list_name", "display_name", "updated", "list_image", "books"]]
                df_lists.append(df_list)

                logger.info(f"Creating df with books")
                df_book = pd.DataFrame(wl["books"])
                df_book["list_id"] = wl["list_id"]
                df_book = df_book[[
                    "list_id", 
                    "title", 
                    "author", 
                    "book_uri", 
                    "primary_isbn10",
                    "publisher", 
                    "rank", 
                    "weeks_on_list", 
                    "created_date"
                ]]
                
                df_books.append(df_book)

            current_date += timedelta(days=7)
            time.sleep(12)
        
df_final_lists = pd.concat(df_lists)
df_final_lists = df_final_lists.drop_duplicates()

df_final_books = pd.concat(df_books)
df_final_books = df_final_books.drop_duplicates()

schema = os.getenv("SF_SCHEMA")

conn = sf.connect(**SF_CREDENTIALS)

success, nchunks, nrows, _ = write_pandas(
        conn,
        df_final_lists,
        schema =  SF_CREDENTIALS["schema"],
        table_name="LISTS",
        auto_create_table=True,
        overwrite=True
    )

logger.info(f"Successfully loaded {nrows} rows into RAW.LISTS in Snowflake")

success, nchunks, nrows, _ = write_pandas(
        conn,
        df_final_books,
        schema =  SF_CREDENTIALS["schema"],
        table_name="BOOKS",
        auto_create_table=True,
        overwrite=True
    )

print(f"Successfully loaded {nrows} rows into RAW.BOOKS Snowflake")