# Datalake Framework

## Storage

Storage objects can be manipulated with an implementation of `AbstractStorage`.

> Currently AWS and GCP are supported

```python
from datalake.provider.aws import Storage

# Create a new Storage instance for a bucket:
storage = Storage("my-bucket")
```

### Detect objects

```python
# Check if an object exists or not
assert storage.exists("path/to/file.csv")

# List all files from a prefix
for key in storage.keys_iterator(search_path):
    print(key)
```

### Copy, move or delete objects

```python
# Copy a file in the same bucket
storage.copy("source/file.csv", "target/file.csv")
# Copy a file in another bucket
storage.copy("source/file.csv", "target/file.csv", "my-bucket")

# Copy a file in the same bucket
storage.move("source/file.csv", "target/file.csv")
# Copy a file in another bucket
storage.move("source/file.csv", "target/file.csv", "my-bucket")

# Delete an obejct
storage.delete("temp/file.csv")
```

### Download and Upload files

```python
# Download an object to a local file
storage.download("path/to/object.csv", "/local/path/to/file.csv")

# Upload a local file to an object
storage.upload("/local/path/to/file.csv", "path/to/object.csv")
```

### Read data from object

```python
# Stream lines from an object
line_iterator = storage.stream("path/to/object.csv")
for row in csv.reader(line_iterator, "datalake"):
    print(row)
```