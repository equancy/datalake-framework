import pytest
from tests.providers.storage import *
from datalake.provider.gcp import Storage


@pytest.fixture
def storage():
    return Storage("eqlab-datamock-ephemeral")


@pytest.fixture
def persisted():
    return Storage("eqlab-datamock-persist")


def test_unknown_bucket():
    with pytest.raises(ValueError):
        return Storage("unknown-bucket")
