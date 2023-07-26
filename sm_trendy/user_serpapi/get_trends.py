import datetime

from cloudpathlib import AnyPath
from loguru import logger
from serpapi import GoogleSearch

from sm_trendy.user_serpapi.config import SerpAPIConfig

params = {
    "api_key": "b6eec35caefee2b187dbb303b4ab1092e513d7e8a8ded4359cc22d03b9bdeba2",
    "engine": "google_trends",
    "q": "Coffee",
    "geo": "DE",
    "data_type": "TIMESERIES",
    "tz": "120",
    "cat": "0",
    "date": "today 5-y",
}

search = GoogleSearch(params)
results = search.get_dict()


class Download:
    """Download trend using config

    :params parent_folder: parent folder for the data
    :param snapshot_date: snapshot date for the path
    :param trends_service: trend service
    """

    def __init__(
        self,
        parent_folder: AnyPath,
        snapshot_date: datetime.date,
        trends_service: GoogleSearch,
    ):
        self.parent_folder = parent_folder
        self.snapshot_date = snapshot_date
        self.trends_service = trends_service

    def __call__(self, config: SerpAPIConfig):
        """
        :param config: config for the keyword
        """
        pass

        # trend_params = config.trend_params
        # path_params = config.path_params
        # target_folder = path_params.path(parent_folder=self.parent_folder)
        # sdf = StoreDataFrame(
        #     target_folder=target_folder, snapshot_date=self.snapshot_date
        # )

        # logger.info(
        #     f"keyword: {trend_params.keyword}\n" f"target_path: {target_folder}\n" "..."
        # )

        # st = SingleTrend(
        #     trends_service=self.trends_service,
        #     keyword=trend_params.keyword,
        #     geo=trend_params.geo,
        #     timeframe=trend_params.timeframe,
        #     cat=trend_params.cat,
        # )

        # sdf.save(st, formats=["csv", "parquet"])
        # logger.info(f"Saved to {target_folder}")
