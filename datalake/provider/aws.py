from datalake.interface import AbstractStorage
import boto3
import botocore

COPY_TRANSFER_THRESHOLD = 5 * 1024 ** 3  # 5 GiB


class Storage(AbstractStorage):
    def __init__(self, bucket):
        self._s3_client = boto3.client("s3")
        self._bucket = bucket
        try:
            self._s3_client.head_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as boto_error:
            if boto_error.response["Error"]["Code"] in ("404", "403"):
                raise ValueError(
                    f"Bucket {bucket} doesn't exist or you don't have permissions to access it"
                )
            raise  # pragma: no cover

    def exists(self, key):
        try:
            self._s3_client.head_object(Bucket=self._bucket, Key=key)
            return True
        except botocore.exceptions.ClientError as boto_error:
            if boto_error.response["Error"]["Code"] == "404":
                return False
            raise  # pragma: no cover

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
        if source_size > COPY_TRANSFER_THRESHOLD:
            mpu = self._s3_client.create_multipart_upload(Bucket=bucket, Key=dst)
            mpu_id = mpu["UploadId"]
            try:
                parts = range(int(source_size / COPY_TRANSFER_THRESHOLD))
                uploaded_parts = []
                for part in parts:
                    start = part * COPY_TRANSFER_THRESHOLD
                    end = (part + 1) * COPY_TRANSFER_THRESHOLD - 1
                    partrange = f"bytes={start}-{end}"
                    result = self._s3_client.upload_part_copy(
                        Bucket=bucket,
                        Key=dst,
                        CopySource=copy_source,
                        CopySourceRange=partrange,
                        PartNumber=part + 1,
                        UploadId=mpu_id,
                    )
                    uploaded_parts.append(
                        {
                            "ETag": result["CopyPartResult"]["ETag"],
                            "PartNumber": part + 1,
                        }
                    )
                start = len(parts) * COPY_TRANSFER_THRESHOLD
                end = source_size - 1
                partrange = f"bytes={start}-{end}"
                result = self._s3_client.upload_part_copy(
                    Bucket=bucket,
                    Key=dst,
                    CopySource=copy_source,
                    CopySourceRange=partrange,
                    PartNumber=len(parts) + 1,
                    UploadId=mpu_id,
                )
                uploaded_parts.append(
                    {
                        "ETag": result["CopyPartResult"]["ETag"],
                        "PartNumber": len(parts) + 1,
                    }
                )

                self._s3_client.complete_multipart_upload(
                    Bucket=bucket,
                    Key=dst,
                    MultipartUpload={"Parts": uploaded_parts},
                    UploadId=mpu_id,
                )
            except Exception:
                self._s3_client.abort_multipart_upload(
                    Bucket=bucket, Key=dst, UploadId=mpu_id
                )
                raise
        else:
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

    def stream(self, key):
        response = self._s3_client.get_object(Bucket=self._bucket, Key=key)
        for line in response["Body"].iter_lines():
            line = line.replace(b"\x00", b"")
            yield line.decode(response["ContentEncoding"])
