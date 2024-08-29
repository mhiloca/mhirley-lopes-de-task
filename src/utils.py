from datetime import timedelta
import pandas as pd
import numpy as np

from src.main import published_dates_miss


def create_dfs(source_list: list, target_list: list, add_columns: dict) -> None:
    for source in source_list:
        df_results = pd.DataFrame(add_columns, index=[0])
        df_temp = pd.DataFrame(source)

        df1_repeated = pd.DataFrame(np.repeat(df_results.values, len(df_temp), axis=0), columns=df_results.columns)
        merged_df = pd.concat([df1_repeated, df_temp], axis=1)
        target_list.append(merged_df)


def create_final_df(source_list: list, unique_id: list) -> pd.DataFrame:
    df = pd.concat(source_list)
    return df.drop_duplicates(subset=unique_id)


def generate_week_dates(start_date, num_weeks):
    dates = []
    current_date = start_date
    for _ in range(num_weeks):
        dates.append(current_date)
        current_date += timedelta(weeks=1)
    return dates

def clean_missing_dates(dates: list, published_dates_miss: list):
    dates.clear()
    for i in range(len(published_dates_miss)):
        dates.append(published_dates_miss[i])
    published_dates_miss.clear()

