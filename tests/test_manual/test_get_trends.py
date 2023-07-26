from sm_trendy.manual.get_trends import ManualSingleTrend


def test_manual_single_trend(serpapi_config_bundle, data_directory):
    path_params = serpapi_config_bundle[0].path_params
    manual_config_folders = data_directory / "manual" / "manual_config"

    mst = ManualSingleTrend(
        path_params=path_params, manual_folder=manual_config_folders
    )

    df = mst.dataframe

    for c in ["date", "query", "extracted_value"]:
        assert c in df
