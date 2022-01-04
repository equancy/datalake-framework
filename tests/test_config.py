import pytest
from tests import *
import datalake.exceptions
from datalake.interface import IStorage, IMonitor


def test_wrong_config():
    with pytest.raises(datalake.exceptions.BadConfiguration) as e:
        Datalake("http://localhost:8080/")
    assert "must be a dict" in str(e.value)

    with pytest.raises(datalake.exceptions.BadConfiguration) as e:
        Datalake({"catalog_url": "http://localhost:5000"})
    assert "catalog URL is invalid" in str(e.value)


def test_valid_config(datalake_config):
    d = Datalake(datalake_config)
    assert d.provider == "local"

    s = d.get_storage("any-bucket")
    assert isinstance(s, IStorage)

    m = d.monitor
    assert isinstance(m, IMonitor)


def test_bad_monitoring(catalog_url):
    with pytest.raises(datalake.exceptions.BadConfiguration) as e:
        Datalake({"catalog_url": catalog_url, "monitoring": False})
    assert "Monitoring configuration must be a dict" in str(e.value)


def test_monitoring_module(catalog_url):
    with pytest.raises(datalake.exceptions.BadConfiguration) as e:
        Datalake({"catalog_url": catalog_url, "monitoring": {"class": "not.a.module.impossible"}})
    assert "Monitor module cannot be found" in str(e.value)

    Datalake({"catalog_url": catalog_url, "monitoring": {"class": "nomonitor", "params": {"quiet": False}}})


def test_monitoring_class(catalog_url):
    with pytest.raises(datalake.exceptions.BadConfiguration) as e:
        Datalake({"catalog_url": catalog_url, "monitoring": {"class": "tests.injection.monitoring.not_exist"}})
    assert "Monitor class cannot be found" in str(e.value)

    with pytest.raises(datalake.exceptions.BadConfiguration) as e:
        Datalake({"catalog_url": catalog_url, "monitoring": {"class": "tests.injection.monitoring.invalid"}})
    assert "Monitor class is not a subclass" in str(e.value)

    with pytest.raises(datalake.exceptions.BadConfiguration) as e:
        Datalake({"catalog_url": catalog_url, "monitoring": {"class": "tests.injection.monitoring.valid"}})
    assert "Monitor class cannot be instanciated" in str(e.value)

    Datalake(
        {
            "catalog_url": catalog_url,
            "monitoring": {
                "class": "tests.injection.monitoring.valid",
                "params": {"dummy": None},
            },
        }
    )
