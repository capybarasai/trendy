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

from sm_trendy.use_pytrends.config import Config
from sm_trendy.utilities.storage import StoreDataFrame


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


class Download:
    """Download trend using config

    ```python
    config_file = ...
    cb = ConfigBundle(file_path=config_file)

    today = datetime.date.today()
    global_request_params = cb.global_config["request"]
    parent_folder = cb.global_config["path"]["parent_folder"]
    trends_service = _TrendReq(
        hl=global_request_params["hl"],
        tz=global_request_params["tz"],
        timeout=(10, 14),
        requests_args={"headers": get_random_user_agent()},
        proxies=["https://157.245.27.9:3128"],
    )

    dl = Download(
        parent_folder=parent_folder,
        snapshot_date=today,
        trends_service=trends_service,
    )

    wait_seconds_min_max = (30, 120)

    for c in cb:
        dl(c)
        wait_seconds = random.randint(*wait_seconds_min_max)
        logger.info(f"Waiting for {wait_seconds} seconds ...")
        time.sleep(wait_seconds)
    ```

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
