from urllib.parse import unquote_plus
import json
import hashlib
import boto3
import botocore
from datalake.interface import IStorage, IStorageEvent, ISecret


class Storage(IStorage):
    def __init__(self, bucket):
        self._s3_client = boto3.client("s3")
        self._bucket = bucket
        try:
            self._s3_client.head_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as boto_error:
            if boto_error.response["Error"]["Code"] in ("404", "403"):
                raise ValueError(f"Bucket {bucket} doesn't exist or you don't have permissions to access it")
            raise  # pragma: no cover

    def __repr__(self):  # pragma: no cover
        return f"s3://{self._bucket}"

    @property
    def name(self):
        return self._bucket

    def exists(self, key):
        try:
            self._s3_client.head_object(Bucket=self._bucket, Key=key)
            return True
        except botocore.exceptions.ClientError as boto_error:
            if boto_error.response["Error"]["Code"] == "404":
                return False
            raise  # pragma: no cover

    def checksum(self, key):
        response = self._s3_client.get_object(Bucket=self._bucket, Key=key)
        m = hashlib.sha256()
        for chunk in response["Body"].iter_chunks():
            m.update(chunk)
        return m.hexdigest()

    def is_folder(self, key):
        obj = self._s3_client.head_object(Bucket=self._bucket, Key=key)
        return obj["ContentType"] == "application/x-directory; charset=UTF-8"

    def keys_iterator(self, prefix):
        paginator = self._s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self._bucket, Prefix=prefix)
        for page in page_iterator:
            if page["KeyCount"] > 0:
                for content in page["Contents"]:
                    yield content["Key"]

    def upload(self, src, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        self._s3_client.upload_file(
            Filename=src,
            Bucket=self._bucket,
            Key=dst,
            ExtraArgs={
                "ContentType": content_type,
                "ContentEncoding": encoding,
                "Metadata": metadata,
            },
        )

    def download(self, src, dst):
        self._s3_client.download_file(Bucket=self._bucket, Key=src, Filename=dst)

    def copy(self, src, dst, bucket=None):
        copy_source = {"Bucket": self._bucket, "Key": src}
        bucket = self._bucket if bucket is None else bucket

        source = self._s3_client.head_object(Bucket=self._bucket, Key=src)
        source_size = source["ContentLength"]
        self._s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=dst)

    def delete(self, key):
        self._s3_client.delete_object(Bucket=self._bucket, Key=key)

    def move(self, src, dst, bucket=None):
        self.copy(src, dst, bucket)
        self.delete(src)

    def put(self, content, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        self._s3_client.put_object(
            Body=content.encode(encoding),
            Bucket=self._bucket,
            Key=dst,
            ContentType=content_type,
            ContentEncoding=encoding,
            Metadata=metadata,
        )

    def get(self, key):
        response = self._s3_client.get_object(Bucket=self._bucket, Key=key)
        return response["Body"].read().decode(response["ContentEncoding"])

    def stream(self, key, encoding="utf-8"):
        response = self._s3_client.get_object(Bucket=self._bucket, Key=key)
        for line in response["Body"].iter_lines():
            line = line.replace(b"\x00", b"")
            yield line.decode(encoding)

    def size(self, key):
        return self._s3_client.head_object(Bucket=self._bucket, Key=key)["ContentLength"]


class StorageEvents:  # pragma: no cover
    def __init__(self, queue, processor, max_message=1, accept_folder=False):
        if queue is None or len(queue) == 0:
            raise ValueError("SQS queue must be defined")
        if not isinstance(processor, IStorageEvent):
            raise ValueError("The event processor is not from the correct class")

        self._processor = processor
        self._max_msg = max_messages
        self._accept_folder = accept_folder

        self._queue = boto3.resource("sqs").get_queue_by_name(QueueName=queue)

    def batch(self):
        messages = self._queue.receive_messages(
            MaxNumberOfMessages=self._max_msg,
            WaitTimeSeconds=20,
        )
        for message in messages:
            try:
                self._preprocess(message)
            finally:
                message.delete()

    def daemon(self):
        while True:
            self.batch()

    def _preprocess(self, message):

        event = json.loads(message.body)

        # Eventually unfold SNS envelope
        if "Type" in event and event["Type"] == "Notification":
            event = json.loads(event["Message"])

        # S3 Event Notifications structure
        # see: https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
        if "Records" not in event or len(event["Records"]) == 0:
            return
        record = event["Records"][0]
        if record["eventSource"] != "aws:s3":
            return

        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        self._processor.process(Storage(bucket), key)


class Secret(ISecret):
    def __init__(self, name):
        client = boto3.client("secretsmanager")
        try:
            secret = client.get_secret_value(SecretId=name)
            if "SecretString" not in secret:
                raise ValueError("Unsupported secret format '{name}'")
            self._secret = secret["SecretString"]
        except botocore.exceptions.ClientError:
            raise ValueError(f"Secret {name} doesn't exist or you don't have permissions to access it")

    @property
    def plain(self):
        return self._secret

    @property
    def json(self):
        return json.loads(self._secret)
