from datalake.interface import IStorage
import hashlib
import os
import shutil
from glob import glob


class Storage(IStorage):
    def __init__(self, bucket):
        self._bucket = bucket
        self._local = os.path.abspath(os.path.expanduser(bucket))

    def __repr__(self):  # pragma: no cover
        return f"file://{self._local}"

    @property
    def name(self):
        return self._bucket

    def exists(self, key):
        return os.path.isfile(os.path.join(self._local, key))

    def checksum(self, key):
        path = os.path.join(self._local, key)
        m = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(1024)
                if not chunk:
                    break
                m.update(chunk)
        return m.hexdigest()

    def is_folder(self, key):
        return os.path.isdir(os.path.join(self._local, key))

    def keys_iterator(self, prefix):
        base = os.path.join(self._local, prefix)
        result = []
        for p in glob(base + "*"):
            result.append(os.path.relpath(p, self._local))
        return result

    def upload(self, src, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        dst_path = os.path.join(self._local, dst)
        dst_parent = os.path.dirname(dst_path)
        os.makedirs(dst_parent)
        shutil.copy(src, dst_path)

    def download(self, src, dst):
        src_path = os.path.join(self._local, src)
        shutil.copy(src_path, dst)

    def copy(self, src, dst, bucket=None):
        src_path = os.path.join(self._local, src)
        dst_path = os.path.join(self._local, dst)
        dst_parent = os.path.dirname(dst_path)
        os.makedirs(dst_parent)
        shutil.copy(src_path, dst_path)

    def delete(self, key):
        os.remove(os.path.join(self._local, key))

    def move(self, src, dst, bucket=None):
        src_path = os.path.join(self._local, src)
        dst_path = os.path.join(self._local, dst)
        dst_parent = os.path.dirname(dst_path)
        os.makedirs(dst_parent)
        shutil.move(src_path, dst_path)

    def put(self, content, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        dst_path = os.path.join(self._local, dst)
        dst_parent = os.path.dirname(dst_path)
        os.makedirs(dst_parent)
        with open(dst_path, "w", encoding=encoding) as f:
            f.write(content)

    def get(self, key):
        path = os.path.join(self._local, key)
        with open(path, "r") as f:
            return f.read()

    def stream(self, key, encoding="utf-8"):
        path = os.path.join(self._local, key)
        with open(path, "r", encoding=encoding) as f:
            for line in f.readlines():
                yield line.replace("\n", "")
