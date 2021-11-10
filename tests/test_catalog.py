import pook
import pytest
import shutil
import tempfile
from datalake import Datalake
from datalake.provider.local import Storage
import datalake.exceptions

CATALOG_URL = "http://catalog.datalake.local"
CATALOG_CONFIG = {
    "provider": "local",
    "csv_format": {
        "delimiter": ",",
        "line_break": "\n",
        "quote_char": '"',
        "double_quote": True,
        "escape_char": "\\",
    },
}
pook.get(f"{CATALOG_URL}/configuration").persist().reply(200).json(CATALOG_CONFIG)


@pytest.fixture(scope="module")
def temp_dir():
    temp_dir = tempfile.mkdtemp(prefix="datalake-framework_", suffix="_local_storage")
    yield temp_dir
    shutil.rmtree(temp_dir)


@pook.on
def test_config(temp_dir):
    pook.get(f"{CATALOG_URL}/catalog/entry/valid").reply(200).json({"feed": "mock"})
    pook.get(f"{CATALOG_URL}/catalog/entry/invalid").reply(404)

    d = Datalake(CATALOG_URL)
    assert d.provider == "local"

    entry = d.get_catalog("valid")
    assert isinstance(entry, dict)

    with pytest.raises(datalake.exceptions.EntryNotFound):
        d.get_catalog("invalid")

    s = d.get_storage(temp_dir)
    assert isinstance(s, Storage)


@pook.on
def test_storage_path(temp_dir):
    pook.get(f"{CATALOG_URL}/storage/trash/input/my-file.csv").reply(200).json(
        {
            "bucket": "/tmp/unit-test",
            "path": ".Trash/input/my-file.csv",
            "uri": "file:///tmp/unit-test/.Trash/input/my-file.csv",
        }
    )
    pook.get(f"{CATALOG_URL}/storage/invalid/input/my-file.csv").reply(404)

    d = Datalake(CATALOG_URL)
    res = d.resolve_path("trash", "input/my-file.csv")
    # returns a Storage instance and the resolved path
    assert isinstance(res, tuple), "Response should be a tuple"
    assert (
        res[0] == "/tmp/unit-test"
    ), "The storage should be from the bucket returned by the API"
    assert (
        res[1] == ".Trash/input/my-file.csv"
    ), "The path should be the one returned by the API"
    assert (
        res[2] == "file:///tmp/unit-test/.Trash/input/my-file.csv"
    ), "The URI should be the one returned by the API"

    # Unknown store should raise an error
    with pytest.raises(datalake.exceptions.StoreNotFound):
        d.resolve_path("invalid", "input/my-file.csv")
