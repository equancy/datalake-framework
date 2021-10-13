import pytest
from datalake.provider.aws.storage import Storage

BUCKET = "eqlab-datalake-landing"


@pytest.fixture
def storage():
    return Storage(BUCKET)


def test_exist(storage):
    assert storage.exists("input/roshi.png")
    assert not storage.exists("not_exists")
