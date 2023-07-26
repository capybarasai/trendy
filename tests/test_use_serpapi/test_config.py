from sm_trendy.use_serpapi.config import (
    SerpAPIConfig,
    SerpAPIConfigBundle,
    SerpAPIParams,
)


def test_serpapi_params():
    sp = SerpAPIParams(
        **{
            "api_key": "",
            "q": "Coffee",
            "geo": "DE",
            "data_type": "TIMESERIES",
            "tz": "120",
        }
    )

    sp_no_geo = SerpAPIParams(
        **{
            "api_key": "",
            "q": "Coffee",
            "data_type": "TIMESERIES",
            "tz": "120",
        }
    )

    sp_set_none = SerpAPIParams(
        **{
            "api_key": "",
            "q": "Coffee",
            "geo": None,
            "data_type": "TIMESERIES",
            "tz": "120",
        }
    )

    assert sp.model_dump(exclude_none=True) == {
        "api_key": "",
        "engine": "google_trends",
        "q": "Coffee",
        "geo": "DE",
        "data_type": "TIMESERIES",
        "tz": "120",
        "date": "today 5-y",
    }

    assert sp_no_geo.model_dump(exclude_none=True) == {
        "api_key": "",
        "engine": "google_trends",
        "q": "Coffee",
        "data_type": "TIMESERIES",
        "tz": "120",
        "date": "today 5-y",
    }

    assert sp_set_none.model_dump(exclude_none=True) == {
        "api_key": "",
        "engine": "google_trends",
        "q": "Coffee",
        "data_type": "TIMESERIES",
        "tz": "120",
        "date": "today 5-y",
    }


def test_serpapi_config():
    raw_config = {
        "serpapi": {
            "api_key": "",
            "date": "today 5-y",
            "cat": "0",
            "geo": "DE",
            "q": "phone case",
            "tz": "120",
        }
    }

    sc = SerpAPIConfig.from_dict(raw_config)

    assert hasattr(sc, "serpapi_params")
    assert hasattr(sc, "path_params")


def test_serpapi_config_bundle(data_directory):

    config_path = data_directory / "use_serpapi" / "test_serpapi_config.json"

    scb = SerpAPIConfigBundle(file_path=config_path, serpapi_key="abc")

    assert len(scb) == 2
