import json
import os
import pytest
import requests
import shutil
import tempfile
from datalake import Datalake
from datalake.provider.local import Storage
import datalake.exceptions
from datalake.interface import IStorage


@pytest.fixture(scope="module")
def catalog_url():
    url = os.getenv("CATALOG_URL", "http://localhost:8080")
    authorization = {
        "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmcmVzaCI6ZmFsc2UsImlhdCI6MTYzMzkzNjU5NCwianRpIjoiNWQxMWNhN2MtMDVlNC00OTdkLWE0MmItZjRhNzgwYzZmYzI1IiwidHlwZSI6ImFjY2VzcyIsInN1YiI6Ik11YWRpYiIsIm5iZiI6MTYzMzkzNjU5NCwicm9sZSI6ImFkbWluIn0.49dawf9sAbrAxE1aac5aXMHzKxPHXM7FMnznb5UbO6I"
    }
    with open("tests/files/storage.json", "r") as f:
        requests.put(f"{url}/storage", headers=authorization, json=json.load(f))

    with open("tests/files/catalog.json", "r") as f:
        requests.post(f"{url}/catalog/import?truncate", headers=authorization, json=json.load(f))
    return url


def test_config(catalog_url):
    d = Datalake(catalog_url)
    assert d.provider == "local"

    entry = d.get_entry("valid")
    assert isinstance(entry, dict)

    with pytest.raises(datalake.exceptions.EntryNotFound):
        d.get_entry("invalid")

    s = d.get_storage("any-bucket")
    assert isinstance(s, Storage)


def test_resolve_path(catalog_url):
    d = Datalake(catalog_url)
    res = d.resolve_path("trash", "my-file.csv")
    # returns a Storage instance and the resolved path
    assert isinstance(res, tuple), "Response should be a tuple"
    assert res[0] == "trash-bucket", "The bucket should be the one returned by the API"
    assert res[1] == "my-file.csv", "The path should be the one returned by the API"
    assert res[2] == "file://trash-bucket/my-file.csv", "The URI should be the one returned by the API"

    # Unknown store should raise an error
    with pytest.raises(datalake.exceptions.StoreNotFound):
        d.resolve_path("invalid", "my-file.csv")


def test_identify(catalog_url):
    d = Datalake(catalog_url)
    res = d.identify("unit-test/equancy/mock/2049-12-06/2049-12-06_mock.csv")
    assert res == ("valid", {"date": "2049-12-06"})

    with pytest.raises(datalake.exceptions.EntryNotFound) as e:
        d.identify("somewhere/over/the.rainbow")
    assert "No entry" in str(e.value)

    with pytest.raises(datalake.exceptions.EntryNotFound) as e:
        d.identify("unit-test/equancy/duplicate.csv")
    assert "Multiple entry" in str(e.value)


def test_entry_path(catalog_url):
    d = Datalake(catalog_url)
    res = d.get_entry_path("valid")
    assert res == "unit-test/equancy/mock/"

    res = d.get_entry_path("valid", {"date": "2056-05-23"})
    assert res == "unit-test/equancy/mock/2056-05-23/2056-05-23_mock.csv"

    with pytest.raises(datalake.exceptions.EntryNotFound):
        d.get_entry_path("invalid")


def test_entry_path_resolved(catalog_url):
    d = Datalake(catalog_url)
    res = d.get_entry_path_resolved("stronghold", "valid")
    assert issubclass(res[0].__class__, IStorage)
    assert res[0].name == "data-bucket"
    assert res[1] == "stronghold/unit-test/equancy/mock/"

    res = d.get_entry_path_resolved("stronghold", "valid", {"date": "2056-05-23"})
    assert issubclass(res[0].__class__, IStorage)
    assert res[0].name == "data-bucket"
    assert res[1] == "stronghold/unit-test/equancy/mock/2056-05-23/2056-05-23_mock.csv"

    with pytest.raises(datalake.exceptions.EntryNotFound):
        d.get_entry_path_resolved("stronghold", "invalid")

    with pytest.raises(datalake.exceptions.StoreNotFound):
        d.get_entry_path_resolved("unknown", "valid")
