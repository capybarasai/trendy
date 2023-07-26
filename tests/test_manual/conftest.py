import pytest

from sm_trendy.use_serpapi.config import SerpAPIConfigBundle


@pytest.fixture
def serpapi_config_bundle(data_directory):

    config_path = data_directory / "use_serpapi" / "test_serpapi_config.json"

    scb = SerpAPIConfigBundle(file_path=config_path, serpapi_key="abc")

    return scb
