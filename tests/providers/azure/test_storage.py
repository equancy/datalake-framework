import pytest
from tests.providers.storage import *
from datalake.provider.azure import Storage
from datalake.exceptions import ContainerNotFound


@pytest.fixture
def storage():
    return Storage("datamock.ephemeral")


@pytest.fixture
def persisted():
    return Storage("datamock.persist")


def test_unknown_bucket():
    with pytest.raises(ContainerNotFound):
        return Storage("datamock.unkown")

    with pytest.raises(ContainerNotFound):
        return Storage("unknown.unkown")
