import pytest
from datalake import Datalake
from datalake.provider.aws import Storage
import datalake.exceptions

def test_config():
    d = Datalake("http://127.0.0.1:5000")
    assert d.provider == "aws"

    entry = d.get_catalog("easy")
    assert isinstance(entry, dict)

    with pytest.raises(datalake.exceptions.EntryNotFound):
        d.get_catalog("not-found")
    # s = d.get_storage("eqlab-datalake-landing")
    # assert isinstance(s, Storage)
