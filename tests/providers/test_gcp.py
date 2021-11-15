import pytest
from tests.storage import *
from datalake.provider.gcp import Storage


@pytest.fixture
def bucket_name():
    return "eqlab-datamock-ephemeral"


@pytest.fixture
def storage(bucket_name):
    return Storage(bucket_name)


@pytest.fixture
def persisted():
    return Storage("eqlab-datamock-persist")


def test_unknown_bucket():
    with pytest.raises(ValueError):
        return Storage("unknown-bucket")
