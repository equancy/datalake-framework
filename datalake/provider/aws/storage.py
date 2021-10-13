from datalake.interface import AbstractStorage
import boto3
import botocore


class Storage(AbstractStorage):
    def __init__(self, bucket):
        self._s3_client = boto3.client("s3")
        self._bucket = bucket
        try:
            self._s3_client.head_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as boto_error:
            if boto_error.response["Error"]["Code"] == "404":
                raise ValueError(
                    f"Bucket {bucket} doesn't exist or you don't have permissions to access it"
                )
            raise # pragma: no cover

    def exists(self, key):
        try:
            self._s3_client.head_object(Bucket=self._bucket, Key=key)
            return True
        except botocore.exceptions.ClientError as boto_error:
            if boto_error.response["Error"]["Code"] == "404":
                return False
            raise # pragma: no cover
