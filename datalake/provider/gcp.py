from datalake.interface import AbstractStorage
import google.cloud.exceptions
from google.cloud import storage as google_storage


class Storage(AbstractStorage):
    def __init__(self, bucket):
        self._client = google_storage.Client()
        try:
            self._bucket = self._client.get_bucket(bucket)
        except google.cloud.exceptions.NotFound:
            raise ValueError(
                f"Bucket {bucket} doesn't exist or you don't have permissions to access it"
            )

    def exists(self, key):
        return self._bucket.get_blob(key) is not None

    def keys_iterator(self, prefix):
        for blob in list(self._client.list_blobs(self._bucket, prefix=prefix)):
            yield blob.name

    def upload(self, src, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        blob = google_storage.blob.Blob(dst, self._bucket)
        blob.upload_from_filename(src)
        blob.content_encoding = encoding
        blob.content_type = content_type
        blob.metadata = metadata
        blob.patch()

    def download(self, src, dst):
        blob = self._bucket.get_blob(src)
        blob.download_to_filename(dst)

    def copy(self, src, dst, bucket=None):
        blob = self._bucket.get_blob(src)
        bucket = self._bucket if bucket is None else self._client.get_bucket(bucket)
        self._bucket.copy_blob(blob, bucket, new_name=dst)

    def delete(self, key):
        blob = self._bucket.get_blob(key)
        self._bucket.delete_blob(key)

    def move(self, src, dst, bucket=None):
        self.copy(src, dst, bucket)
        self.delete(src)

    def put(self, content, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        blob = google_storage.blob.Blob(dst, self._bucket)
        with blob.open("wb") as f:
            f.write(content.encode(encoding))
        blob.content_encoding = encoding
        blob.content_type = content_type
        blob.metadata = metadata
        blob.patch()

    def get(self, key):
        blob = self._bucket.get_blob(key)
        return blob.download_as_bytes().decode(blob.content_encoding)

    def stream(self, key):
        blob = self._bucket.get_blob(key)
        with blob.open("rt", encoding=blob.content_encoding) as f:
            line = f.readline()
            while line != "":
                yield line.replace("\n", "")
                line = f.readline()
