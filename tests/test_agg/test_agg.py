import json
from pathlib import Path

import pytest

from sm_trendy.aggregate.agg import AggAPIJSON, DownloadedLoader
from sm_trendy.utilities.config import PathParams


@pytest.fixture
def agg_dll_path_params():
    return PathParams(
        **{"timeframe": "today 5-y", "cat": "0", "geo": "DE", "keyword": "phone case"}
    )


@pytest.fixture
def downloaded_data_reloaded(data_directory, agg_dll_path_params):
    parent_folder = data_directory / "aggregate" / "serpapi_downloaded"
    dll = DownloadedLoader(parent_folder=parent_folder)
    df = dll(agg_dll_path_params)

    return df


@pytest.fixture
def agg_api_json_records(data_directory):
    with open(
        data_directory / "aggregate" / "test_agg_api_json_records.json", "r"
    ) as fp:
        records = json.load(fp)

    records = sorted(records, key=lambda x: x["date"])

    return records


def test_downloaded_loader(data_directory, downloaded_data_reloaded):
    assert len(downloaded_data_reloaded) > 0

    for c in ["query", "extracted_value", "date"]:
        assert c in downloaded_data_reloaded


def test_agg_api_json(downloaded_data_reloaded, agg_api_json_records):
    apj = AggAPIJSON()
    agg_records = apj(downloaded_data_reloaded, sort_by="date")
    agg_records = sorted(agg_records, key=lambda x: x["date"])
    agg_records

    for r, g in zip(agg_records, agg_api_json_records):
        assert r == g, f"generate {r} is not the same as expected {g}"
