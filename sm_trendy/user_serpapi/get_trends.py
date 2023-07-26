import datetime
from functools import cached_property
from typing import Dict, Union

import pandas as pd
from cloudpathlib import AnyPath
from loguru import logger
from serpapi import GoogleSearch

from sm_trendy.user_serpapi.config import SerpAPIConfig
from sm_trendy.utilities.storage import StoreDataFrame

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


class SerpAPISingleTrend:
    def __init__(self, serpapi_params):
        self.serpapi_params = serpapi_params

    @cached_property
    def search_results(self):
        api_params = self.serpapi_params.model_dump(exclude_none=True)

        search = GoogleSearch(api_params)
        results = search.get_dict()

        self._check_status(results["search_metadata"]["status"])

        return results

    def _check_status(self, status: str):
        if not status == "Success":
            raise Exception(f"Request failed with error {status}")

    @cached_property
    def dataframe(self) -> pd.DataFrame:
        """
        Build the dataframe

        :param keyword: keyword to be searched
        """
        timeline_data = self.search_results["interest_over_time"]["timeline_data"]

        df = pd.DataFrame(
            sum([self._flatten_record(record) for record in timeline_data], [])
        )

        df["date"] = pd.to_datetime(df.timestamp, unit="s", origin="unix")

        return df

    @staticmethod
    def _flatten_record(record: Dict):
        """Convert raw record to flattened dicts"""
        return [
            {**i, **{"date_range": record["date"], "timestamp": record["timestamp"]}}
            for i in record["values"]
        ]

    @cached_property
    def metadata(self) -> Dict[str, Dict]:
        return {
            "search_metadata": self.search_results["search_metadata"],
            "search_parameters": self.search_results["search_parameters"],
        }


class SerpAPIDownload:
    """Download trend using config

    :params parent_folder: parent folder for the data
    :param snapshot_date: snapshot date for the path
    :param trends_service: trend service
    """

    def __init__(
        self,
        parent_folder: AnyPath,
        snapshot_date: datetime.date,
    ):
        self.parent_folder = parent_folder
        self.snapshot_date = snapshot_date

    def __call__(self, config: SerpAPIConfig):
        """
        :param config: config for the keyword
        """
        api_params = config.serpapi_params.model_dump(exclude_none=True)

        path_params = config.path_params
        target_folder = path_params.path(parent_folder=self.parent_folder)

        sdf = StoreDataFrame(
            target_folder=target_folder, snapshot_date=self.snapshot_date
        )

        logger.info(
            f"keyword: {config.serpapi_params.q}\n"
            f"target_path: {target_folder}\n"
            "..."
        )

        sst = SerpAPISingleTrend(serpapi_params=api_params)

        sdf.save(sst, formats=["csv", "parquet"])
        logger.info(f"Saved to {target_folder}")
