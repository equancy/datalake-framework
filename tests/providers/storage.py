import os
import tempfile
import pytest
from uuid import uuid4
import hashlib

ROSHI_SHA256 = "91acc7bf7c8f87535e2f7c64050a2805379e014e33697c589dd1a90c55e81057"
SCHILLER_SHA256 = "f2e04ea5ecb18c45c604a30206417848f80fc21f445d032043c23e268c6a3875"


@pytest.fixture(scope="module")
@pytest.mark.providers
def test_id():
    return uuid4()


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


@pytest.mark.providers
def test_exist(persisted):
    assert persisted.exists("roshi.png")
    assert not persisted.exists("not-exists.dat")


@pytest.mark.providers
def test_checksum(persisted):
    assert persisted.checksum("roshi.png") == ROSHI_SHA256


@pytest.mark.providers
def test_size(persisted):
    assert persisted.size("roshi.png") == 109376


@pytest.mark.providers
def test_folder(persisted):
    assert persisted.is_folder("empty-folder/")
    assert not persisted.is_folder("roshi.png")


@pytest.mark.providers
def test_listing(persisted):
    s = {k for k in persisted.keys_iterator("an-die-freude/")}
    assert not s ^ {
        "an-die-freude/part-1.txt",
        "an-die-freude/part-2.txt",
        "an-die-freude/part-3.txt",
        "an-die-freude/part-4.txt",
        "an-die-freude/part-5.txt",
    }

    s = {k for k in persisted.keys_iterator("nowhere/")}
    assert len(s) == 0


@pytest.mark.providers
def test_upload(storage, test_id):
    key = f"{test_id}/schiller/ode-to-joy.txt"
    storage.upload(
        "./tests/files/schiller.txt",
        key,
        content_type="text/plain",
        metadata={
            "author": "Friedrich Schiller",
            "title": "An die Freude",
            "written": "1785",
        },
    )
    assert storage.exists(key)
    assert storage.checksum(key) == SCHILLER_SHA256


@pytest.mark.providers
def test_download(persisted, temp_file):
    persisted.download("roshi.png", temp_file)
    m = hashlib.sha256()
    with open(temp_file, "rb") as f:
        m.update(f.read())
    assert m.hexdigest() == ROSHI_SHA256


@pytest.mark.providers
def test_copy(storage, test_id):
    src = f"{test_id}/schiller/ode-to-joy.txt"
    dst = f"{test_id}/beethoven/symphony-9.txt"
    assert storage.exists(src)
    assert not storage.exists(dst)
    storage.copy(src, dst)
    assert storage.exists(src)
    assert storage.exists(dst)
    assert storage.checksum(src) == storage.checksum(dst)


@pytest.mark.providers
def test_copy_across(persisted, storage, test_id):
    # Copy across buckets
    src = "big-one/ODD_CSV_V2.zip"
    dst = f"{test_id}/big-one/ODD_CSV_V2.zip"
    assert persisted.exists(src)
    assert not storage.exists(dst)
    persisted.copy(src, dst, storage.name)
    assert persisted.exists(src)
    assert storage.exists(dst)
    assert persisted.checksum(src) == storage.checksum(dst)


@pytest.mark.providers
def test_delete(storage, test_id):
    key = f"{test_id}/schiller/ode-to-joy.txt"
    assert storage.exists(key)
    storage.delete(key)
    assert not storage.exists(key)


@pytest.mark.providers
def test_move(storage, test_id):
    src = f"{test_id}/beethoven/symphony-9.txt"
    dst = f"{test_id}/classics/an-die-freude.txt"
    assert storage.exists(src)
    assert not storage.exists(dst)
    checksum = storage.checksum(src)
    storage.move(src, dst, storage.name)
    assert not storage.exists(src)
    assert storage.exists(dst)
    assert checksum == storage.checksum(dst)


@pytest.mark.providers
def test_put(storage, test_id, text):
    key = f"{test_id}/friedrich/ode-to-joy.txt"
    storage.put(
        text,
        key,
        content_type="text/plain",
        encoding="utf-8",
        metadata={
            "author": "Friedrich Schiller",
            "title": "An die Freude",
        },
    )
    assert storage.exists(key)


@pytest.mark.providers
def test_get(storage, test_id, text):
    key = f"{test_id}/friedrich/ode-to-joy.txt"
    data = storage.get(key)
    assert data == text


@pytest.mark.providers
def test_stream(storage, test_id, text):
    key = f"{test_id}/friedrich/ode-to-joy.txt"
    data = [l for l in storage.stream(key)]
    assert data == text.split("\n")
