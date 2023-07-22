from collections import OrderedDict
from pathlib import Path

import pytest

from sm_trendy.config import PathParams


@pytest.fixture
def path_params():
    return PathParams(
        keyword="phone case", category="all", country="DE", frequency="1W"
    )


def test_path_params_order():
    pp = PathParams(
        keyword="phone case",
        country="DE",
        frequency="1W",
        category="all",
    )

    orders = ["keyword", "category", "country", "frequency"]
    for i, j in zip(pp.model_fields.keys(), orders):
        assert i == j


def test_path_params_path(path_params):

    assert path_params.path_schema == OrderedDict(
        [
            ("keyword", "phone-case"),
            ("category", "all"),
            ("country", "DE"),
            ("frequency", "1W"),
        ]
    )

    assert path_params.path(Path("/tmp")) == Path(
        "/tmp/keyword=phone-case/category=all/country=DE/frequency=1W"
    )
