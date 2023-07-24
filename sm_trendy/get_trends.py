import contextlib
import datetime
import json
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import pandas as pd
import requests
from cloudpathlib import AnyPath
from loguru import logger
from pytrends.request import TrendReq

from sm_trendy.config import Config


@contextlib.contextmanager
def _requests_get_as_post():
    requests.get, requests_get = requests.post, requests.get
    try:
        yield
    finally:
        requests.get = requests_get


class _TrendReq(TrendReq):
    """A fix to the original package

    Ref: [](https://github.com/GeneralMills/
    pytrends/pull/563#issuecomment-1466338941)
    """

    def GetGoogleCookie(self):
        # TODO: make sure to get rid of this dirty hack
        with _requests_get_as_post():
            return super().GetGoogleCookie()


class SingleTrend:
    """Get the trend for one keyword, in one country

    `trends_service` can be instatiated using

    ```python
    trends_service = _TrendReq(hl="en-US", tz=120)
    ```

    :param trends_service: pytrends TrendReq
    :param geo: geo code such as `"DE"`
    :param timeframe: the time frame of the trend, see pytrends
    :param cat: category code, see pytrends for a list.
    """

    def __init__(
        self,
        trends_service: _TrendReq,
        keyword: str,
        geo: str,
        timeframe: str = "today 5-y",
        cat: int = 0,
    ):
        self.trends_service = trends_service
        self.keyword = keyword
        self.cat = cat
        self.geo = geo
        self.timeframe = timeframe

        self.timestamp = datetime.datetime.now()

    @cached_property
    def dataframe(self) -> pd.DataFrame:
        """
        Build the dataframe

        :param keyword: keyword to be searched
        """

        if not isinstance(self.keyword, str):
            raise Exception(f"Requires str as keyword input, got {type(self.keyword)}")

        logger.debug(f"building payload for {self.keyword} ...")
        self.trends_service.build_payload(
            [self.keyword], cat=self.cat, timeframe=self.timeframe, geo=self.geo
        )

        logger.debug(f"Downloading trend for {self.keyword} ...")
        df = self.trends_service.interest_over_time()

        return df

    @cached_property
    def metadata(self) -> Dict[str, Union[str, int]]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "keyword": self.keyword,
            "geo": self.geo,
            "timeframe": self.timeframe,
            "cat": self.cat,
        }


class StoreDataFrame:
    """Save dataframe

    We follow the schema

    ```python
    target_folder / "format=parquet" / "snapshot_date="
    ```

    `target_folder` should reflect what data is inside.
    For example, we may use

    ```
    target_folder = some_folder / "google_trend"
    / "keyword=phone_case" / "category=all"
    / "country=DE" / "frequency=1W"
    ```

    :param target_folder: parent folder for the data.
        Note that subfolders will be created inside it.
    :param snapshot_date: the date when the data was produced.
        Please stick to UTC date.
    """

    def __init__(self, target_folder: AnyPath, snapshot_date: datetime.date):
        self.target_folder = target_folder

        if not isinstance(snapshot_date, datetime.date):
            raise TypeError(
                f"snapshot_date provided is not date time: {type(snapshot_date)}"
            )
        else:
            self.snapshot_date = snapshot_date

    def save(
        self,
        trend_data: SingleTrend,
        formats: Optional[List[Literal["parquet", "csv"]]],
    ):
        """
        Save the trend results

        :param trend_data: the object containing the dataframe and metadata
        :param formats: which formats to save as
        """
        df = trend_data.dataframe
        metadata = trend_data.metadata

        format_dispatcher = {
            "parquet": {
                "method": self._save_parquet,
                "path": self._file_path(format="parquet"),
            },
            "csv": {
                "method": self._save_csv,
                "path": self._file_path(format="csv"),
            },
        }

        for f in formats:
            try:
                f_method = format_dispatcher[f]["method"]
                f_path_data = format_dispatcher[f]["path"]["data"]  # type: ignore
                f_path_metadata = format_dispatcher[f]["path"]["metadata"]  # type: ignore

                f_method(dataframe=df, target_path=f_path_data)  # type: ignore
                self._save_metadata(metadata=metadata, target_path=f_path_metadata)
            except Exception as e:
                logger.error(f"can not save format {f}: {e}")

        df = trend_data.dataframe

    def _file_path(self, format: Literal["parquet", "csv"]) -> Dict[str, AnyPath]:
        """Compute the full path for the target file
        based on the format

        :param format: the file format to be used
        """
        folder = (
            self.target_folder
            / f"format={format}"
            / f"snapshot_date={self.snapshot_date.isoformat()}"
        )
        folder.mkdir(parents=True, exist_ok=True)
        return {
            "data": folder / f"data.{format}",
            "metadata": folder / "metadata.json",
        }

    def _save_parquet(self, dataframe: pd.DataFrame, target_path: AnyPath):
        """save a dataframe as parquet

        :param dataframe: dataframe to be saved as file
        :param target_path: the target file full path
        """
        dataframe.to_parquet(target_path)

    def _save_csv(self, dataframe: pd.DataFrame, target_path: AnyPath):
        """save a dataframe as csv

        :param dataframe: dataframe to be saved as file
        :param target_path: the target file full path
        """
        dataframe.to_csv(target_path, index=False)

    def _save_metadata(self, metadata: Dict, target_path: AnyPath):
        """save metadata as a json file

        :param metadata: metadata in dictionary format
        :param target_path:
        """

        with open(target_path, "w") as fp:
            json.dump(metadata, fp, indent=2)


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
        trends_service: _TrendReq,
    ):
        self.parent_folder = parent_folder
        self.snapshot_date = snapshot_date
        self.trends_service = trends_service

    def __call__(self, config: Config):
        """
        :param config: config for the keyword
        """

        trend_params = config.trend_params
        path_params = config.path_params
        target_folder = path_params.path(parent_folder=self.parent_folder)
        sdf = StoreDataFrame(
            target_folder=target_folder, snapshot_date=self.snapshot_date
        )

        logger.info(
            f"keyword: {trend_params.keyword}\n" f"target_path: {target_folder}\n" "..."
        )

        st = SingleTrend(
            trends_service=self.trends_service,
            keyword=trend_params.keyword,
            geo=trend_params.geo,
            timeframe=trend_params.timeframe,
            cat=trend_params.cat,
        )

        sdf.save(st, formats=["csv", "parquet"])
        logger.info(f"Saved to {target_folder}")
