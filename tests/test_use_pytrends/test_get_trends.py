import datetime

import pandas as pd
import pytest

from sm_trendy.use_pytrends.get_trends import SingleTrend, StoreDataFrame, _TrendReq


@pytest.fixture
def pytrends_service(mocker, test_directory):
    class MockedTrendReq:
        def __init__(self):
            pass

        def build_payload(
            self, kw_list, cat=0, timeframe="today 5-y", geo="", gprop=""
        ):
            pass

        def interest_over_time(self):
            df = pd.read_csv(test_directory / "data" / "test_keyword_motion_sensor.csv")
            return df

    return MockedTrendReq()


@pytest.fixture
def singletrend(pytrends_service):
    pt = pytrends_service
    st = SingleTrend(trends_service=pt, keyword="motion sensor", geo="DE")

    return st


def test_single_trend(singletrend):

    # pt = _TrendReq(hl="en-US", tz=120)
    # https://forbrains.co.uk/international_tools/earth_timezones

    df = singletrend.dataframe

    assert set(df.columns) == {"date", "motion sensor", "isPartial"}


def test_store_dataframe(tmp_path, singletrend):
    target_folder = tmp_path / "store_dataframe_folder"
    sdf = StoreDataFrame(
        target_folder=target_folder, snapshot_date=datetime.date(2023, 6, 1)
    )

    sdf.save(trend_data=singletrend, formats=["csv", "parquet"])

    csv_files = list(target_folder.rglob("*.csv"))
    parquet_files = list(target_folder.rglob("*.parquet"))

    assert csv_files != []
    assert parquet_files != []

    df_csv_reloaded = pd.read_csv(csv_files[0])
    df_parquet_reloaded = pd.read_parquet(parquet_files[0])

    pd.testing.assert_frame_equal(singletrend.dataframe, df_parquet_reloaded)
    pd.testing.assert_frame_equal(df_csv_reloaded, df_parquet_reloaded)
