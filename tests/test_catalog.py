import pytest
import requests
import shutil
import tempfile
from tests.fixtures import *
from datalake import Datalake
from datalake.provider.local import Storage
import datalake.exceptions
from datalake.interface import IStorage


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

    res = d.get_entry_path("valid", {"date": "2056-05-23"}, strict=True)
    assert res == "unit-test/equancy/mock/2056-05-23/2056-05-23_mock.csv"

    with pytest.raises(ValueError) as e:
        res = d.get_entry_path("valid", strict=True)
    assert str(e.value).startswith("Missing parameters")

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
