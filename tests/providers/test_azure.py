import pytest
from tests.storage import *
from datalake.provider.azure import Storage
from datalake.exceptions import ContainerNotFound


@pytest.fixture
def storage():
    return Storage("https://datamock.blob.core.windows.net/ephemeral")


@pytest.fixture
def persisted():
    return Storage("https://datamock.blob.core.windows.net/persist")


def test_unknown_bucket():
    with pytest.raises(ContainerNotFound):
        return Storage("https://datamock.blob.core.windows.net/unkown")

    with pytest.raises(ContainerNotFound):
        return Storage("https://unknown.blob.core.windows.net/unkown")


# Override default test: folders don't exist in Azure
def test_folder(storage):
    assert not storage.is_folder("anything")
