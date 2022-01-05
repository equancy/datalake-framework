from datalake.interface import IStorage, IStorageEvent, ISecret
from datalake.exceptions import ContainerNotFound, DatalakeError
import json
from hashlib import sha256
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient, ContentSettings
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import AzureError
from tempfile import mkstemp
from os import close, remove


class Storage(IStorage):
    def __init__(self, bucket):
        container_url = self._bucket_to_container_url(bucket)
        try:
            self._container = ContainerClient.from_container_url(
                container_url=container_url,
                credential=DefaultAzureCredential(),
            )
            if not self._container.exists():
                raise ContainerNotFound(f"Container {bucket} doesn't exist")
        except AzureError as e:
            raise ContainerNotFound(
                f"Container {bucket} doesn't exist or you don't have permissions to access it ({str(e)})"
            )

    def _bucket_to_container_url(self, bucket):
        self._name = bucket
        bucket_parts = bucket.split(".")
        if len(bucket_parts) != 2:
            raise ValueError(f"Wrong bucket name format '{bucket}'")
        account = bucket_parts[0]
        container = bucket_parts[1]
        return f"https://{account}.blob.core.windows.net/{container}"

    def __repr__(self):  # pragma: no cover
        return self._container.url

    @property
    def name(self):
        return self._name

    def exists(self, key):
        return self._container.get_blob_client(key).exists()

    def checksum(self, key):
        stream = self._container.get_blob_client(key).download_blob()
        m = sha256()
        for chunk in stream.chunks():
            m.update(chunk)
        return m.hexdigest()

    def is_folder(self, key):
        metadata = self._container.get_blob_client(key).get_blob_properties().metadata
        return "hdi_isfolder" in metadata and metadata["hdi_isfolder"]

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
        # if you wonder why this seems so overcomplicated, feel free to ask Microsoft
        # https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python#start-copy-from-url-source-url--metadata-none--incremental-copy-false----kwargs-
        creds = DefaultAzureCredential()
        dest_container_url = self._container.url if bucket is None else self._bucket_to_container_url(bucket)
        dest_container = ContainerClient.from_container_url(container_url=dest_container_url, credential=creds)
        dest_blob = dest_container.get_blob_client(dst)
        source_blob = f"{self._container.url}/{src}"
        
        token = creds.get_token("https://storage.azure.com/.default").token
        response = dest_blob.start_copy_from_url(source_blob, requires_sync=True, source_authorization=f"Bearer {token}")
        if response["copy_status"] != "success":
            raise DatalakeError(f"Azure blob copy failed (ID '{response['copy_id']}')")

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


class Secret(ISecret):
    def __init__(self, name):
        name_parts = name.split(".")
        if len(name_parts) != 2:
            raise ValueError(f"Wrong secret name format '{name}'")
        key_vault = name_parts[0]
        secret_name = name_parts[1]
        try:
            secret_client = SecretClient(
                vault_url=f"https://{key_vault}.vault.azure.net/",
                credential=DefaultAzureCredential(),
            )
            secret = secret_client.get_secret(secret_name)
            self._secret = secret.value
        except AzureError as e:
            raise ValueError(f"Secret {name} doesn't exist or you don't have permissions to access it ({str(e)})")

    @property
    def plain(self):
        return self._secret

    @property
    def json(self):
        return json.loads(self._secret)
