import logging
import requests
import os
import time
import utils

logger = logging.getLogger("NYT_EXTRACTION")
logger.setLevel(logging.INFO)

NYT_API_KEY = os.getenv("NYT_API_KEY")
API_URL = "https://api.nytimes.com/svc/books/v3/lists/overview.json"
DATE_FORMAT = "%Y-%m-%d"


def _fetch_weekly_lists(date: str, api_key: str = NYT_API_KEY, api_url: str = API_URL) -> requests.models.Response:
    request_url = f"{api_url}?published_date={date}&api-key={api_key}"
    request_headers = {"Accept": "application/json"}

    response = requests.get(url=request_url, headers=request_headers)
    return response


def extract_data(dates: list, lists_dfs: list, books_dfs:list, published_dates_miss: list) -> None:
    for dt in dates:
        str_date = dt.strftime(DATE_FORMAT)
        logger.info(f"Fetching data for week of {str_date}")
        response = _fetch_weekly_lists(str_date)

        if response.status_code == 200:
            results = response.json()["results"]
            results_dict = {
                "published_date": results["published_date"],
                "best_sellers_date": results["bestsellers_date"]
            }
            week_lists = results["lists"]
            utils.create_dfs(week_lists, lists_dfs, results_dict)
            logger.info(f"Created Lists DataFrame for weekly data published on {results['published_date']}")
            for week in week_lists:
                results_dict["list_id"] = week["list_id"]
                utils.create_dfs(week["books"], books_dfs, results_dict)
                logger.info(f"Created Books DataFrame for list {week['list_id']}")

        else:
            logger.warning(f"Failed to retrive data for published_date {str_date}")
            published_dates_miss.append(dt)

        time.sleep(12)
