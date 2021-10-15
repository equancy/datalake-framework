from datalake import Datalake
from datalake.provider.aws import Storage


def test_config():
    d = Datalake("http://127.0.0.1:5000")
    assert d.provider == "aws"

    s = d.get_storage("eqlab-datalake-landing")
    assert isinstance(s, Storage)
