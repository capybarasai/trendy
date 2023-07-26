import json

import pytest

from sm_trendy.use_serpapi.get_trends import SerpAPISingleTrend


@pytest.fixture
def serpapi_search_result(data_directory):

    search_results_path = data_directory / "use_serpapi" / "serpapi_coffee_results.json"

    with open(search_results_path, "r") as fp:
        results = json.load(fp)

    return results


def test_serpapi_single_trend(serpapi_search_result):
    sst = SerpAPISingleTrend(serpapi_params=None)

    sst.search_results = serpapi_search_result

    sst.dataframe


def test_download_convert_dataframe(serpapi_search_result):
    serpapi_search_result
