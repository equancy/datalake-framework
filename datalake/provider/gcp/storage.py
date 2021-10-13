from datalake.interface import AbstractStorage
from google.cloud import storage as google_storage


class Storage(AbstractStorage):
    def __init__(self, bucket):
        self._client = google_storage.Client()
        self._bucket = self._client.get_bucket(bucket)

    def exists(self, key):
        return self._bucket.get_blob(key) is not None
