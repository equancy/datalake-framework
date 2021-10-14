import pytest
from tests.storage import *
from datalake.provider.aws import Storage


@pytest.fixture
def bucket_name():
    return "eqlab-datalake-landing"


@pytest.fixture
def storage(bucket_name):
    return Storage(bucket_name)


def test_unknown_bucket():
    with pytest.raises(ValueError):
        return Storage("unknown-bucket")
