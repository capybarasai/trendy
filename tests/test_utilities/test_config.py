import pytest

from sm_trendy.utilities.config import PathParams


@pytest.fixture
def path_params():
    return PathParams(keyword="curtain", cat="0", geo="DE", timeframe="today 5-y")


def test_path_params_s3(path_params):
    url = path_params.s3_access_point(
        base_url="https://sm-google-trend-public.s3.eu-central-1.amazonaws.com",
        snapshot_date="latest",
        filename="data.json",
    )

    assert url == (
        "https://sm-google-trend-public.s3.eu-central-1.amazonaws.com"
        "/keyword=curtain/cat=0/geo=de/timeframe=today-5-y/snapshot_date=latest/data.json"
    )
