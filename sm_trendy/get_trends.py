import contextlib
from functools import cached_property
from typing import List

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
    :param timeframe: the time frame of the trend
    :param cat: category code, see pytrends.
    """

    def __init__(
        self,
        trends_service: _TrendReq,
        geo: str,
        timeframe: str = "today 5-y",
        cat: int = 0,
    ):
        self.trends_service = trends_service
        self.cat = cat
        self.geo = geo
        self.timeframe = timeframe

    @cached_property
    def dataframe(self, keyword: str) -> pd.DataFrame:
        """
        Build the dataframe

        :param keyword: keyword to be searched
        """

        if isinstance(keyword, str):
            keyword = [keyword]

        pytrends.build_payload(
            keyword, cat=self.cat, timeframe=self.timeframe, geo=self.geo
        )

        df = pytrends.interest_over_time()

        return df


class TrendParams:
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
