import pytest
from tests.providers.secret import *
from datalake.provider.aws import Secret


@pytest.fixture
def plain_secret():
    return Secret("datalake/unittest/georges-sand")


@pytest.fixture
def json_secret():
    return Secret("datalake/unittest/sample")


@pytest.mark.providers
def test_unknown_secret():
    with pytest.raises(ValueError) as e:
        return Secret("unknown-secret")
    assert "doesn't exist" in str(e.value)
