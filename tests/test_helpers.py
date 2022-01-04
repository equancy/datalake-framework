import pytest
import os
from tests import *
from tempfile import mkstemp
from decimal import InvalidOperation
from datalake import Datalake
from datalake.helpers import cast_float, cast_integer, cast_date, cast_time, cast_datetime, DatasetBuilder
from hashlib import sha256


def checksum(path):
    m = sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024)
            if not chunk:
                break
            m.update(chunk)
    return m.hexdigest()


def test_cast_number():
    for k, v in {
        "fr_FR": "01 234 567,890",
        "fr_CH": "01 234 567,890",
        "de_CH": "01’234’567.890",
        "de_DE": "01.234.567,890",
        "en_US": "01,234,567.890",
        "pl": "01 234 567,890",
    }.items():
        assert cast_float(v, k) == 1234567.89
        assert cast_integer(v, k) == 1234567

    with pytest.raises(ValueError) as e:
        cast_float("1 23 456", "en")
    assert "Unable to cast number" in str(e.value)

    assert cast_integer(1234567.89) == 1234567
    assert cast_float(1234567.89) == 1234567.89


def test_cast_date():
    assert cast_date("13/02/2009", ["DD/MM/YYYY"]) == "2009-02-13"
    assert cast_date("13/02/09", ["DD/MM/YY"]) == "2009-02-13"
    assert cast_date("20090213", ["YYYYMMDD"]) == "2009-02-13"
    assert cast_date("1234567890123", ["x"]) == "2009-02-13"

    with pytest.raises(ValueError) as e:
        cast_date("1er janvier", ["DD MMMM"])
    assert "Wrong date format" in str(e.value)


def test_cast_time():
    fmt = ["HH:mm:ss", "HH:mm:ss.SSS", "HH:mm:ssZ", "HH:mm:ss.SSSZ", "HH:mm:ssZZ", "HH:mm:ss.SSSZZ"]
    assert cast_time("14:15:16", fmt) == "14:15:16.000+0000"
    assert cast_time("14:15:16.789", fmt) == "14:15:16.789+0000"
    assert cast_time("14:15:16.789+0200", fmt) == "14:15:16.789+0200"
    assert cast_time("14:15:16.789+02:00", fmt) == "14:15:16.789+0200"

    assert cast_time("233130.123", ["HHmmss.SSS"]) == "23:31:30.123+0000"
    assert cast_time("1234567890123", ["x"]) == "23:31:30.123+0000"

    with pytest.raises(ValueError) as e:
        cast_time("midi moins le quart", fmt)
    assert "Wrong time format" in str(e.value)


def test_cast_datetime():
    fmt = [
        "YYYY-MM-DDTHH:mm:ss",
        "YYYY-MM-DDTHH:mm:ss.SSS",
        "YYYY-MM-DDTHH:mm:ssZ",
        "YYYY-MM-DDTHH:mm:ss.SSSZ",
        "YYYY-MM-DDTHH:mm:ssZZ",
        "YYYY-MM-DDTHH:mm:ss.SSSZZ",
        "YYYY-MM-DD HH:mm:ss",
        "YYYY-MM-DD HH:mm:ss.SSS",
        "YYYY-MM-DD HH:mm:ssZ",
        "YYYY-MM-DD HH:mm:ss.SSSZ",
        "YYYY-MM-DD HH:mm:ssZZ",
        "YYYY-MM-DD HH:mm:ss.SSSZZ",
        "DD/MM/YY HH:mm:ss",
        "DD/MM/YY HH:mm:ss.SSS",
        "DD/MM/YY HH:mm:ssZ",
        "DD/MM/YY HH:mm:ss.SSSZ",
        "DD/MM/YY HH:mm:ssZZ",
        "DD/MM/YY HH:mm:ss.SSSZZ",
        "DD/MM/YYYY HH:mm:ss",
        "DD/MM/YYYY HH:mm:ss.SSS",
        "DD/MM/YYYY HH:mm:ssZ",
        "DD/MM/YYYY HH:mm:ss.SSSZ",
        "DD/MM/YYYY HH:mm:ssZZ",
        "DD/MM/YYYY HH:mm:ss.SSSZZ",
        "DD-MM-YY HH:mm:ss",
        "DD-MM-YY HH:mm:ss.SSS",
        "DD-MM-YY HH:mm:ssZ",
        "DD-MM-YY HH:mm:ss.SSSZ",
        "DD-MM-YY HH:mm:ssZZ",
        "DD-MM-YY HH:mm:ss.SSSZZ",
        "DD-MM-YYYY HH:mm:ss",
        "DD-MM-YYYY HH:mm:ss.SSS",
        "DD-MM-YYYY HH:mm:ssZ",
        "DD-MM-YYYY HH:mm:ss.SSSZ",
        "DD-MM-YYYY HH:mm:ssZZ",
        "DD-MM-YYYY HH:mm:ss.SSSZZ",
    ]
    assert cast_datetime("2049-05-23 14:15:16", fmt) == "2049-05-23T14:15:16.000+0000"
    assert cast_datetime("23-05-49 14:15:16", fmt) == "2049-05-23T14:15:16.000+0000"
    assert cast_datetime("23/05/49 14:15:16.789", fmt) == "2049-05-23T14:15:16.789+0000"
    assert cast_datetime("2049-05-23 14:15:16.789-02:00", fmt) == "2049-05-23T14:15:16.789-0200"

    assert cast_datetime("20090213_233130", ["YYYYMMDD_HHmmss"]) == "2009-02-13T23:31:30.000+0000"
    assert cast_datetime("1234567890123", ["x"]) == "2009-02-13T23:31:30.123+0000"

    with pytest.raises(ValueError) as e:
        cast_datetime("maintenant", fmt)
    assert "Wrong datetime format" in str(e.value)


def test_dataset_builder(datalake_instance):
    with datalake_instance.new_dataset_builder("SANTE-compositions") as dsb:
        dsb.add_sequence(["60493619", "comprimé", "00005", "ACIDE ACÉTYLSALICYLIQUE", "500,00 mg", "un comprimé", "SA", "1"])
        dsb.add_dict(
            {
                "code_cis": "66032721",
                "nature_composant": "SA",
                "code_substance": "00675",
                "denomination_substance": "VALINE",
                "dosage_substance": "3,64 g",
                "designation_pharma": "émulsion",
                "reference_dosage": "1000 ml d'émulsion reconstituée",
                "numero_liaison": "17",
            }
        )

        with pytest.raises(ValueError) as e:
            dsb.add_sequence(["60493619", "comprimé", "00005", "ACIDE ACÉTYLSALICYLIQUE", "500,00 mg", "un comprimé"])
        assert "Row has not the expected column count" in str(e.value)

        with pytest.raises(ValueError) as e:
            dsb.add_dict(
                {
                    "code_cis": "66032721",
                    "code_substance": "00675",
                    "denomination_substance": "VALINE",
                    "dosage_substance": "3,64 g",
                }
            )
        assert "Row has not the expected column count" in str(e.value)

    dataset_path = dsb.path
    assert dsb.row_count == 2
    assert os.path.exists(dataset_path)
    assert checksum(dataset_path) == "603fbc0e5e80b6623c796e7d62e86c01a63145c9d4d07d2a43e5c52cb15ec1c7"

    # Test typing
    (temp_file, temp_path) = mkstemp()
    os.close(temp_file)
    with datalake_instance.new_dataset_builder(
        key="valid",
        path=temp_path,
        lang="fr_FR",
        date_formats=["DD/MM/YYYY", "HH:m:s", "DD/MM/YYYY HH:m:s"],
    ) as dsb:
        r = dsb.new_dict()
        r["column_str"] = " Galiophobia,\u00a0Acarophobia,\u00a0Athazagoraphobia,\u00a0Clinophobia  "
        r["column_int"] = "1\u202F234\u202F567,89"
        r["column_dec"] = "1 234 567,89"
        r["column_date"] = "23/05/2049"
        r["column_time"] = "14:5:8"
        r["column_datetime"] = "23/05/2049 14:5:8"
        dsb.add_dict(r)
    assert dsb.row_count == 1
    assert checksum(temp_path) == "58c985550ad738eca743adfae20dc198fcb995a10ffe0ebf56f38dd2ea963a71"
    os.remove(temp_path)

    # Test typing when dataset is ciphered
    ciphered_dict = {
        "column_str": "R2FsaW9waG9iaWEsIEFjYXJvcGhvYmlhLCBBdGhhemFnb3JhcGhvYmlhLCBDbGlub3Bob2JpYQ==",
        "column_int": "1234567",
        "column_dec": "MTIzNDU2Ny44OQ==",
        "column_date": "MjA0OS0wNS0yMw==",
        "column_time": "14:05:08.000+0000",
        "column_datetime": "2049-05-23T14:05:08.000+0000",
    }
    with datalake_instance.new_dataset_builder(key="valid", ciphered=True) as dsb:
        dsb.add_dict(ciphered_dict)
    dataset_path = dsb.path
    assert dsb.row_count == 1
    assert os.path.exists(dataset_path)
    assert checksum(dataset_path) == "99601c2e125d3e7729fa0371addbf91336d4c61430e72804cc6160c759eb46ed"
    with datalake_instance.new_dataset_builder(key="valid", ciphered=False) as dsb:
        with pytest.raises(ValueError) as e:
            dsb.add_dict(ciphered_dict)
        assert "Unable to cast" in str(e.value)


def test_dataset_reader(datalake_instance):
    dsr = datalake_instance.new_dataset_reader("dataset", "STATS-mises_en_cause", {"year": 2019})
    count = 0
    for item in dsr.iter_list():
        count += 1
        assert len(item) == 4
        assert type(item[0]) == str
        assert type(item[1]) == int
        assert type(item[2]) == int
        assert type(item[3]) == float
    assert count == 10

    for item in dsr.iter_dict():
        assert "intitule" in item
        if item["intitule"] == "Cambriolages":
            assert item["pct_mineurs"] == 30
            assert item["pct_femmes"] == 9
            assert item["ensemble"] == 19.3
