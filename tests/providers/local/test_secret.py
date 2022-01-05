import pytest
from tests.providers.secret import *
from datalake.provider.local import Secret


@pytest.fixture
def plain_secret():
    return Secret("datalake-unittest_georges-sand")


@pytest.fixture
def json_secret():
    return Secret("datalake-unittest_sample")


def test_unknown_secret():
    with pytest.raises(ValueError) as e:
        return Secret("unknown-secret")
    assert "doesn't exist" in str(e.value)
