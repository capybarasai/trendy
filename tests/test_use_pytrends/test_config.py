from collections import OrderedDict
from pathlib import Path

import pytest

from sm_trendy.use_pytrends.config import (
    Config,
    ConfigBundle,
    PathParams,
    RequestParams,
    TrendParams,
)


@pytest.fixture
def path_params():
    return PathParams(keyword="phone case", cat="0", geo="DE", timeframe="today 5-y")


def test_path_params_order():
    pp = PathParams(
        keyword="phone case",
        geo="DE",
        timeframe="1W",
        cat="all",
    )

    orders = ["keyword", "cat", "geo", "timeframe"]
    for i, j in zip(pp.model_fields.keys(), orders):
        assert i == j


def test_path_params_path(path_params):

    assert path_params.path_schema == OrderedDict(
        [
            ("keyword", "phone-case"),
            ("cat", "0"),
            ("geo", "de"),
            ("timeframe", "today-5-y"),
        ]
    )

    assert path_params.path(Path("/tmp")) == Path(
        "/tmp/keyword=phone-case/cat=0/geo=de/timeframe=today-5-y"
    )


@pytest.fixture
def config_dict():
    return {
        "request": {"tz": 120, "hl": "en-US"},
        "trend": {
            "timeframe": "today 5-y",
            "cat": 0,
            "geo": "DE",
            "keyword": "phone case",
        },
        "path": {
            "keyword": "phone case",
            "cat": "0",
            "geo": "DE",
            "timeframe": "today 5-y",
        },
    }


def test_config(config_dict):
    c = Config.from_dict(config=config_dict)

    assert hasattr(c, "request_params")
    assert hasattr(c, "path_params")
    assert hasattr(c, "trend_params")


def test_config_bundle(data_directory):

    config_path = data_directory / "test_config.json"
    cb = ConfigBundle(file_path=config_path)

    assert len(cb) == 2
