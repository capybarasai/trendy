import datetime
import json
from functools import cached_property
from typing import Dict

import pandas as pd
from cloudpathlib import AnyPath
from loguru import logger

from sm_trendy.manual.config import SerpAPI2Manual
from sm_trendy.utilities.config import PathParams
from sm_trendy.utilities.storage import StoreDataFrame


class ManualSingleTrend:
    """get trend from manually downloaded file"""

    def __init__(self, path_params: PathParams, manual_folder: AnyPath):
        self.path_params = path_params
        self.manual_folder = manual_folder

    @cached_property
    def dataframe(self) -> pd.DataFrame:
        """
        Build the dataframe
        """

        temp_path = self.path_params.path(parent_folder=self.manual_folder)
        temp_path.mkdir(parents=True, exist_ok=True)

        with open(temp_path / "manual.json", "r") as fp:
            manual_config = json.load(fp)

        df_downloaded = pd.read_csv(temp_path / "multiTimeline.csv", skiprows=2)
        columns = df_downloaded.columns.tolist()
        value_column = [c for c in columns if c != "Week"][0]

        df_downloaded.rename(
            columns={
                "Week": "date",
                value_column: "extracted_value",
            },
            inplace=True,
        )

        df_downloaded["query"] = manual_config["keyword"]
        df_downloaded["geo"] = manual_config["geo"]
        df_downloaded["timeframe"] = manual_config["timeframe"]
        df_downloaded["cat"] = manual_config["cat"]

        return df_downloaded

    @cached_property
    def metadata(self) -> Dict[str, Dict]:
        return {"path": self.path_params.model_dump()}


class ManualDownload:
    """Download trend using config

    :params parent_folder: parent folder for the data
    :param snapshot_date: snapshot date for the path
    :param trends_service: trend service
    """

    def __init__(
        self,
        parent_folder: AnyPath,
        snapshot_date: datetime.date,
        manual_folder: AnyPath,
    ):
        self.parent_folder = parent_folder
        self.snapshot_date = snapshot_date
        self.manual_folder = manual_folder

    def __call__(self, config):
        """
        :param config: config for the keyword
        """
        path_params = config.path_params
        target_folder = path_params.path(parent_folder=self.parent_folder)

        sdf = StoreDataFrame(
            target_folder=target_folder, snapshot_date=self.snapshot_date
        )
        sst = ManualSingleTrend(
            path_params=path_params, manual_folder=self.manual_folder
        )

        logger.info(
            f"keyword: {config.serpapi_params.q}\n"
            f"target_path: {target_folder}\n"
            "..."
        )

        logger.info(f"Saving to {target_folder} ...")
        sdf.save(sst, formats=["csv", "parquet"])
