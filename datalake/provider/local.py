from datalake.interface import IStorage, ISecret
import json
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
        os.makedirs(dst_parent, exist_ok=True)
        shutil.copy(src, dst_path)

    def download(self, src, dst):
        src_path = os.path.join(self._local, src)
        shutil.copy(src_path, dst)

    def copy(self, src, dst, bucket=None):
        src_path = os.path.join(self._local, src)
        dst_bucket = self._local if bucket is None else os.path.abspath(os.path.expanduser(bucket))
        dst_path = os.path.join(dst_bucket, dst)
        dst_parent = os.path.dirname(dst_path)
        os.makedirs(dst_parent, exist_ok=True)
        shutil.copy(src_path, dst_path)

    def delete(self, key):
        os.remove(os.path.join(self._local, key))

    def move(self, src, dst, bucket=None):
        src_path = os.path.join(self._local, src)
        dst_bucket = self._local if bucket is None else os.path.abspath(os.path.expanduser(bucket))
        dst_path = os.path.join(dst_bucket, dst)
        dst_parent = os.path.dirname(dst_path)
        os.makedirs(dst_parent, exist_ok=True)
        shutil.move(src_path, dst_path)

    def put(self, content, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        dst_path = os.path.join(self._local, dst)
        dst_parent = os.path.dirname(dst_path)
        os.makedirs(dst_parent, exist_ok=True)
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

    def size(self, key):
        return os.path.getsize(os.path.join(self._local, key))

class Secret(ISecret):
    def __init__(self, name):
        secrets_root = os.getenv("DATALAKE_SECRETS_ROOT", "./.secrets")
        secrets_path = os.path.abspath(os.path.expanduser(secrets_root))
        
        try:
            with open(os.path.join(secrets_path, name), "r") as f:
                self._secret = f.read()
        except IOError:
            raise ValueError(f"Secret {name} doesn't exist or you don't have permissions to access it")

    @property
    def plain(self):
        return self._secret

    @property
    def json(self):
        return json.loads(self._secret)
