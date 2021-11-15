from datalake.interface import IStorage, IStorageEvent
import hashlib
import json
import google.auth
import google.cloud.exceptions
from google.cloud import storage as google_storage
from google.cloud import pubsub


class Storage(IStorage):
    def __init__(self, bucket):
        self._client = google_storage.Client()
        try:
            self._bucket = self._client.get_bucket(bucket)
            self._bucket_name = bucket
        except google.cloud.exceptions.NotFound:
            raise ValueError(
                f"Bucket {bucket} doesn't exist or you don't have permissions to access it"
            )

    def __repr__(self):  # pragma: no cover
        return f"gs://{self._bucket.name}"

    @property
    def name(self):
        return self._bucket_name

    def exists(self, key):
        return self._bucket.get_blob(key) is not None

    def checksum(self, key):
        blob = self._bucket.get_blob(key)
        m = hashlib.sha256()
        with blob.open("rb") as f:
            while True:
                chunk = f.read(1024)
                if not chunk:
                    break
                m.update(chunk)
        return m.hexdigest()

    def is_folder(self, key):
        blob = self._bucket.get_blob(key)
        return (
            blob.content_type == "text/plain"
            and blob.size == 0
            and blob.name.endswith("/")
        )

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

    def stream(self, key, encoding="utf-8"):
        blob = self._bucket.get_blob(key)
        with blob.open("rt", encoding=encoding) as f:
            line = f.readline()
            while line != "":
                yield line.replace("\n", "")
                line = f.readline()


class StorageNotifications:  # pragma: no cover
    def __init__(self, subscription, processor, max_messages=1):
        if subscription is None or len(subscription) == 0:
            raise ValueError("PubSub subscription must be defined")
        if not isinstance(processor, IStorageEvent):
            raise ValueError("The event processor is not from the correct class")

        self._processor = processor
        self._max_msg = max_messages

        creds, project = google.auth.default()
        with pubsub.SubscriberClient() as subscriber:
            self._subscription = subscriber.subscription_path(project, subscription)

    def batch(self):
        with pubsub.SubscriberClient() as subscriber:
            response = subscriber.pull(
                request={
                    "subscription": self._subscription,
                    "max_messages": self._max_msg,
                }
            )
            ack = []
            try:
                for msg in response.received_messages:
                    ack.append(msg.ack_id)
                    self._preprocess(msg.message)
            finally:
                if len(ack) > 0:
                    subscriber.acknowledge(
                        request={
                            "subscription": self._subscription,
                            "ack_ids": ack,
                        }
                    )

    def daemon(self):
        def callback(message):
            try:
                self._preprocess(message)
            finally:
                message.ack()

        with pubsub.SubscriberClient() as subscriber:
            future = subscriber.subscribe(self._subscription, callback)
            try:
                future.result()
            finally:
                future.cancel()

    def _preprocess(self, message):
        event = json.loads(message.data.decode("utf-8"))

        # check the message is a storage event
        # see https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations
        if "kind" not in event or event["kind"] != "storage#object":
            return

        bucket = event["bucket"]
        name = event["name"]

        self._processor.process(Storage(bucket), name)
