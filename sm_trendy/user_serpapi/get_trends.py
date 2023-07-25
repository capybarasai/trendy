# %%
from serpapi import GoogleSearch

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

# %%
