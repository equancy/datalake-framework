import os
import json
import pytest
import requests
from datalake import Datalake


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


@pytest.fixture(scope="module")
def datalake_config(catalog_url):
    return {"catalog_url": catalog_url, "monitoring": {"class": "NoMonitor", "params": {"quiet": False}}}


@pytest.fixture(scope="module")
def datalake_instance(datalake_config):
    return Datalake(datalake_config)
