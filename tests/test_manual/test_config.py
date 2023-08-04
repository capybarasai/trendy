from sm_trendy.manual.config import SerpAPI2Manual


def test_serpapi_to_manual(tmp_path, serpapi_config_bundle):
    m = SerpAPI2Manual(manual_folder=tmp_path)

    m(serpapi_config_bundle)

    assert {p.name for p in tmp_path.iterdir()} == {
        "keyword=phone-case",
        "keyword=curtain",
    }
