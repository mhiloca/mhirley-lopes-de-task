from datetime import date
import logging
import pandas as pd
import fetch_nyt_data as fnd
import persistence
import utils


logger = logging.getLogger("NYT_EXTRACTION")
logger.setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

START_DATE = date(2021, 1, 1)
NUM_WEEKS = 156
DATE_FORMAT = "%Y-%m-%d"

books_dfs = []
lists_dfs = []
published_dates_miss = []

if __name__ == "__main__":
    dates = utils.generate_week_dates(START_DATE, NUM_WEEKS)
    fnd.extract_data(dates, lists_dfs, books_dfs, published_dates_miss)

    while published_dates_miss:
        fnd.extract_data(published_dates_miss, lists_dfs, books_dfs, published_dates_miss)

    final_lists_df = utils.create_final_df(source_list=lists_dfs, unique_id=['list_id'])
    final_books_df = utils.create_final_df(source_list=books_dfs, unique_id=['title'])
    final_published_dates_df = pd.DataFrame(dates, columns=["PUBLISHED_DATE"])

    persistence.upload_df(final_lists_df, "LISTS", "RAW")
    persistence.upload_df(final_books_df, "BOOKS", "RAW")
    persistence.upload_df(final_published_dates_df, "PUBLISHED_DATES", "RAW")