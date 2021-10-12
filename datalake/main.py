from google.cloud import storage
import google.cloud.exceptions

# import google.auth

DL_BUCKET = "eqlab-datalake-landing"
storage_client = storage.Client()
bucket = storage_client.get_bucket(DL_BUCKET)


def main():
    # creds, project_id = google.auth.default()
    assert exists("input/roshi.png")
    copy("input/roshi.png", "trash/roshi.png")
    copy("input/roshi.png", "purgatory/roshi.png")
    assert delete("trash/ploup.ploup") == "Not found"
    assert delete("trash/roshi.png") == "OK"
    move("purgatory/roshi.png", "trash/roshi.png")
    download("input/roshi.png", "./roshi-sensei.png")
    upload(
        "./README.md",
        "input/readme.md",
        "text/plain",
        metadata={"language": "markdown"},
    )
    print(get("input/readme.md"))
    put("Peace Love & Death Metal", "input/message.txt", "text/plain")
    for l in stream("input/readme.md"):
        print(l)

    for i in keys_iterator("input/"):
        print(i)


def exists(key):
    return bucket.get_blob(key) is not None


def copy(src_key, dest_key, dest_bucket=None):
    src = bucket.get_blob(src_key)
    dest_bucket = (
        storage_client.get_bucket(DL_BUCKET)
        if dest_bucket is None
        else storage_client.get_bucket(dest_bucket)
    )
    bucket.copy_blob(src, dest_bucket, new_name=dest_key)


def delete(key):
    try:
        blob = bucket.get_blob(key)
        bucket.delete_blob(key)
        return "OK"
    except google.cloud.exceptions.NotFound:
        return "Not found"


def move(src_key, dest_key, dest_bucket=None):
    copy(src_key, dest_key, dest_bucket)
    delete(src_key)


def download(src_key, dest_path):
    blob = bucket.get_blob(src_key)
    blob.download_to_filename(dest_path)


def upload(src_path, dest_key, content_type="text/csv", encoding="utf-8", metadata={}):
    blob = storage.blob.Blob(dest_key, bucket)
    blob.upload_from_filename(src_path)
    blob.content_encoding = encoding
    blob.content_type = content_type
    blob.metadata = metadata
    blob.update()


def get(key):
    blob = bucket.get_blob(key)
    return blob.download_as_bytes().decode(blob.content_encoding)


def put(content, dest_key, content_type="text/csv", encoding="utf-8", metadata={}):
    blob = storage.blob.Blob(dest_key, bucket)
    with blob.open("wb") as f:
        f.write(content.encode(encoding))
    blob.content_encoding = encoding
    blob.content_type = content_type
    blob.metadata = metadata
    blob.update()


def stream(key, encoding="utf-8"):
    blob = storage.blob.Blob(key, bucket)
    with blob.open("rt", encoding=encoding, newline="\n") as f:
        line = f.readline()
        while line != "":
            yield line
            line = f.readline()


def keys_iterator(prefix):
    lb = list(storage_client.list_blobs(bucket, prefix=prefix))
    for b in lb:
        yield b.name


if __name__ == "__main__":
    main()
