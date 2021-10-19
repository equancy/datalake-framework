import pytest
import shutil
import tempfile
from datalake import Datalake
from datalake.provider.local import Storage
import datalake.exceptions


@pytest.fixture(scope="module")
def temp_dir():
    temp_dir = tempfile.mkdtemp(prefix="datalake-framework_", suffix="_local_storage")
    yield temp_dir
    shutil.rmtree(temp_dir)


def test_config(temp_dir):
    d = Datalake("http://127.0.0.1:5000")
    assert d.provider == "local"

    entry = d.get_catalog("easy")
    assert isinstance(entry, dict)

    with pytest.raises(datalake.exceptions.EntryNotFound):
        d.get_catalog("not-found")

    s = d.get_storage(temp_dir)
    assert isinstance(s, Storage)
