import os
import tempfile
import pytest

UPLOAD_FILE = "./tests/files/roshi.png"
UPLOAD_KEY = "storage/upload.dat"
COPIED_KEY = "storage/copied.dat"
MOVED_KEY = "storage/moved.dat"


@pytest.fixture
def temp_file():
    fd, path = tempfile.mkstemp(prefix="datalake-framework_", suffix="_unit-test")
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def text():
    with open("./tests/files/schiller.txt", "r") as f:
        s = f.read()
    return s


def test_exist(storage):
    assert storage.exists("input/roshi.png")
    assert not storage.exists("not_exists")


def test_listing(storage):
    s = {k for k in storage.keys_iterator("input/")}
    assert not s ^ {"input/roshi.png"}

    s = {k for k in storage.keys_iterator("nowhere/")}
    assert len(s) == 0


def test_upload(storage):
    storage.upload(
        UPLOAD_FILE,
        UPLOAD_KEY,
        content_type="image/png",
        metadata={"phase": "unit-test"},
    )
    assert storage.exists(UPLOAD_KEY)


def test_download(storage, temp_file):
    storage.download(UPLOAD_KEY, temp_file)
    with open(temp_file, "rb") as f:
        downloaded = f.read()
    with open(UPLOAD_FILE, "rb") as f:
        original = f.read()
    assert downloaded == original


def test_copy(storage, bucket_name):
    storage.copy(UPLOAD_KEY, COPIED_KEY, bucket_name)
    assert storage.exists(COPIED_KEY)


def test_delete(storage):
    storage.delete(UPLOAD_KEY)
    assert not storage.exists(UPLOAD_KEY)


def test_move(storage, bucket_name):
    storage.move(COPIED_KEY, MOVED_KEY, bucket_name)
    assert not storage.exists(COPIED_KEY)
    assert storage.exists(MOVED_KEY)


def test_put(storage, text):
    storage.put(
        text,
        UPLOAD_KEY,
        content_type="text/plain",
        encoding="utf-8",
        metadata={"phase": "unit-test"},
    )
    assert storage.exists(UPLOAD_KEY)


def test_get(storage, text):
    data = storage.get(UPLOAD_KEY)
    assert data == text


def test_stream(storage, text):
    data = [l for l in storage.stream(UPLOAD_KEY)]
    assert data == text.split("\n")


def test_folder(storage):
    assert storage.is_folder("trash/")
    assert not storage.is_folder("input/roshi.png")
