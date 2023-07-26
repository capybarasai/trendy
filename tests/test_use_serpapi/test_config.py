from sm_trendy.user_serpapi.config import SerpAPIConfig


def test_serpapi_config():
    sc = SerpAPIConfig(
        **{
            "api_key": "",
            "q": "Coffee",
            "geo": "DE",
            "data_type": "TIMESERIES",
            "tz": "120",
        }
    )

    sc_no_geo = SerpAPIConfig(
        **{
            "api_key": "",
            "q": "Coffee",
            "data_type": "TIMESERIES",
            "tz": "120",
        }
    )

    sc_set_none = SerpAPIConfig(
        **{
            "api_key": "",
            "q": "Coffee",
            "geo": None,
            "data_type": "TIMESERIES",
            "tz": "120",
        }
    )

    assert sc.dict(exclude_none=True) == {
        "api_key": "",
        "engine": "google_trends",
        "q": "Coffee",
        "geo": "DE",
        "data_type": "TIMESERIES",
        "tz": "120",
        "date": "today 5-y",
    }

    assert sc_no_geo.dict(exclude_none=True) == {
        "api_key": "",
        "engine": "google_trends",
        "q": "Coffee",
        "data_type": "TIMESERIES",
        "tz": "120",
        "date": "today 5-y",
    }

    assert sc_set_none.dict(exclude_none=True) == {
        "api_key": "",
        "engine": "google_trends",
        "q": "Coffee",
        "data_type": "TIMESERIES",
        "tz": "120",
        "date": "today 5-y",
    }
