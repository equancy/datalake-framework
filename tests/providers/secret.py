import pytest
import json


@pytest.fixture(scope="module")
def expected_text():
    with open("./tests/files/secret.txt", "r") as f:
        return f.read()


@pytest.fixture(scope="module")
def expected_json():
    with open("./tests/files/secret.json", "r") as f:
        return json.load(f)


@pytest.mark.providers
def test_plain(plain_secret, expected_text):
    s = plain_secret.plain
    assert s == expected_text


@pytest.mark.providers
def test_json(json_secret, expected_json):
    s = json_secret.json
    assert s == expected_json
