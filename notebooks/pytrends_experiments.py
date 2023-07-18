# %% # PyTrends Experiments

# %%
from pytrends.request import TrendReq

import contextlib

import requests


class _TrendReq(TrendReq):
    def GetGoogleCookie(self):
        # TODO: make sure to get rid of this dirty hack
        with _requests_get_as_post():
            return super().GetGoogleCookie()


@contextlib.contextmanager
def _requests_get_as_post():
    requests.get, requests_get = requests.post, requests.get
    try:
        yield
    finally:
        requests.get = requests_get


# https://forbrains.co.uk/international_tools/earth_timezones
pytrends = _TrendReq(hl="en-US", tz=120)

# %%
kw_list = ["zalando"]
pytrends.build_payload(kw_list, cat=0, timeframe="today 5-y", geo="DE")

# %%
df = pytrends.interest_over_time()
# %%
