from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_directory():
    return Path(__file__).parent.resolve()


@pytest.fixture(scope="session")
def data_directory(test_directory):
    return test_directory / "data"
