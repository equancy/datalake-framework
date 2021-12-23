from datalake.interface import IStorage, IStorageEvent
from datalake.exceptions import ContainerNotFound
from hashlib import sha256
from azure.storage.blob import ContainerClient, ContentSettings
from azure.core.exceptions import AzureError
from tempfile import mkstemp
from os import close, remove


class Storage(IStorage):  # pragma: no cover
    def __init__(self, bucket):
        try:
            self._container = ContainerClient.from_container_url(
                bucket, credential="nbFKOQZFQa96gLZ0DE0PLC9zGOiUdhIKHyjASVePhrtwPpjnCRkRIduHn4dVbQCWjP5yOKaWU1xxg3/Q8LzYXA=="
            )
            if not self._container.exists():
                raise ContainerNotFound(f"Container {bucket} doesn't exist")
        except AzureError:
            raise ContainerNotFound(f"Container {bucket} doesn't exist or you don't have permissions to access it")

    def __repr__(self):  # pragma: no cover
        return self._container.url

    @property
    def name(self):
        return self._container.url

    def exists(self, key):
        return self._container.get_blob_client(key).exists()

    def checksum(self, key):
        stream = self._container.get_blob_client(key).download_blob()
        m = sha256()
        for chunk in stream.chunks():
            m.update(chunk)
        return m.hexdigest()

    def is_folder(self, key):
        return False

    def keys_iterator(self, prefix):
        for blob in self._container.list_blobs(name_starts_with=prefix):
            yield blob.name

    def upload(self, src, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        with open(src, "rb") as data:
            self._container.upload_blob(
                name=dst,
                data=data,
                overwrite=True,
                metadata=metadata,
                content_settings=ContentSettings(content_type=content_type, content_encoding=encoding),
                encoding=encoding,
            )

    def download(self, src, dst):
        with open(dst, "wb") as f:
            self._container.download_blob(src).readinto(f)

    def copy(self, src, dst, bucket=None):
        # https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-copy?tabs=python#copy-a-blob
        pass

    def delete(self, key):
        self._container.delete_blob(key)

    def move(self, src, dst, bucket=None):
        self.copy(src, dst, bucket)
        self.delete(src)

    def put(self, content, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        self._container.upload_blob(
            name=dst,
            data=content,
            overwrite=True,
            metadata=metadata,
            content_settings=ContentSettings(content_type=content_type, content_encoding=encoding),
            encoding=encoding,
        )

    def get(self, key):
        blob = self._container.get_blob_client(key)
        encoding = blob.get_blob_properties().content_settings.content_encoding
        return blob.download_blob().readall().decode(encoding)

    def stream(self, key, encoding="utf-8"):
        (temp_file, temp_path) = mkstemp(prefix=f"datalake-storage_", suffix=".azure")
        close(temp_file)
        try:
            self.download(key, temp_path)
            with open(temp_path, "r", encoding=encoding) as f:
                for line in f.readlines():
                    yield line.replace("\n", "")
        finally:
            remove(temp_path)

    def size(self, key):
        return self._container.get_blob_client(key).get_blob_properties().size
