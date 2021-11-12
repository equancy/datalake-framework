import json
import os
import pytest
import requests
import shutil
import tempfile
from datalake import Datalake
from datalake.provider.local import Storage
import datalake.exceptions


@pytest.fixture(scope="module")
def catalog_url():
    url = os.getenv("CATALOG_URL", "http://localhost:8080")
    authorization = {
        "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmcmVzaCI6ZmFsc2UsImlhdCI6MTYzMzkzNjU5NCwianRpIjoiNWQxMWNhN2MtMDVlNC00OTdkLWE0MmItZjRhNzgwYzZmYzI1IiwidHlwZSI6ImFjY2VzcyIsInN1YiI6Ik11YWRpYiIsIm5iZiI6MTYzMzkzNjU5NCwicm9sZSI6ImFkbWluIn0.49dawf9sAbrAxE1aac5aXMHzKxPHXM7FMnznb5UbO6I"
    }
    with open("tests/files/storage.json", "r") as f:
        requests.put(f"{url}/storage", headers=authorization, json=json.load(f))

    with open("tests/files/catalog.json", "r") as f:
        requests.post(
            f"{url}/catalog/import?truncate", headers=authorization, json=json.load(f)
        )
    return url


def test_config(catalog_url):
    d = Datalake(catalog_url)
    assert d.provider == "local"

    entry = d.get_catalog("valid")
    assert isinstance(entry, dict)

    with pytest.raises(datalake.exceptions.EntryNotFound):
        d.get_catalog("invalid")

    s = d.get_storage("any-bucket")
    assert isinstance(s, Storage)


def test_storage_path(catalog_url):
    d = Datalake(catalog_url)
    res = d.resolve_path("trash", "my-file.csv")
    # returns a Storage instance and the resolved path
    assert isinstance(res, tuple), "Response should be a tuple"
    assert res[0] == "trash-bucket", "The bucket should be the one returned by the API"
    assert res[1] == "my-file.csv", "The path should be the one returned by the API"
    assert (
        res[2] == "file://trash-bucket/my-file.csv"
    ), "The URI should be the one returned by the API"

    # Unknown store should raise an error
    with pytest.raises(datalake.exceptions.StoreNotFound):
        d.resolve_path("invalid", "my-file.csv")
