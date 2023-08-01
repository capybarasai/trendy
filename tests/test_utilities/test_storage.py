import pytest

from sm_trendy.utilities.storage import StoreJSON


@pytest.fixture
def test_storage_records():
    return [{"name": "A", "value": 1}, {"name": "B", "value": 2}]


def test_store_json(tmp_path, test_storage_records):
    target_path = tmp_path / "test_store_json"
    target_path.mkdir(parents=True, exist_ok=True)

    sj = StoreJSON(target_folder=target_path, snapshot_date="latest")

    sj.save(records=test_storage_records, formats=["json"])

    assert [
        i.name for i in (target_path / "format=json" / "snapshot_date=latest").iterdir()
    ] == ["data.json"]
