import contextlib
import datetime
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Dict, List, Literal, Optional

import pandas as pd
import requests
from pytrends.request import TrendReq


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

        pytrends.build_payload(
            [self.keyword], cat=self.cat, timeframe=self.timeframe, geo=self.geo
        )

        df = pytrends.interest_over_time()

        return df

    @cached_property
    def metadata(self) -> Dict[str, str]:
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

    def __init__(self, target_folder: Path, snapshot_date: datetime.date):
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

        format_dispatcher = {
            "parquet": {
                "method": self._save_parquet,
                "path": self._path_parquet,
            },
            "csv": {
                "method": self._save_csv,
                "path": self._path_csv,
            },
        }

    @property
    def _path_parquet(self) -> Path:
        return (
            self.target_folder
            / "format=parquet"
            / f"snapshot_date={self.snapshot_date.isoformat()}"
            / "data.parquet"
        )

    @property
    def _path_csv(self) -> Path:
        return (
            self.target_folder
            / "format=csv"
            / f"snapshot_date={self.snapshot_date.isoformat()}"
            / "data.csv"
        )

    def _save_parquet(self, dataframe: pd.DataFrame, target_path: Path):
        """save a dataframe as parquet"""
        dataframe.to_parquet(target_path)

    def _save_csv(self, dataframe: pd.DataFrame, target_path: Path):
        """save a dataframe as parquet"""
        dataframe.to_csv(target_path)


@dataclass
class TrendReqParams:
    """
    Parameters for trend

    1. Get tz info from [here](https://forbrains.co.uk/international_tools/earth_timezones).
    """

    tz: int = 120
    hl: str = "en-US"


# https://forbrains.co.uk/international_tools/earth_timezones
pytrends = _TrendReq(hl="en-US", tz=120)

kw_list = ["zalando"]
pytrends.build_payload(kw_list, cat=0, timeframe="today 5-y", geo="DE")


df = pytrends.interest_over_time()
