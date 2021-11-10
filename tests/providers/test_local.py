import os
import shutil
import tempfile
import pytest
from tests.storage import *
from datalake.provider.local import Storage


@pytest.fixture(scope="module")
def bucket_name():
    temp_dir = tempfile.mkdtemp(prefix="datalake-framework_", suffix="_local_storage")
    input_dir = os.path.join(temp_dir, "input")
    os.mkdir(input_dir)
    trash_dir = os.path.join(temp_dir, "trash")
    os.mkdir(trash_dir)
    shutil.copy("./tests/files/roshi.png", input_dir)
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def storage(bucket_name):
    return Storage(bucket_name)
