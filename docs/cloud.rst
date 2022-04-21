Cloud resources
===============

The Datalake framework helps to use cloud resources without implementing the cloud provider's SDK directly.

You can start building data pipelines locally and deliver them in the cloud without a single change in your code.
Currently the following cloud providers are supported:

- **Amazon Web Service**
- **Azure**
- **Google Cloud Platform**
- **local** is a special implementation for mocking cloud resources and is meant for local development or unit-testing

:py:obj:`datalake.ServiceDiscovery` works like a factory for finding the correct implementation for your provider.

For example, you can work with AWS::

    from datalake import ServiceDiscovery

    aws_provider = ServiceDiscovery("aws")
    
    # Use a S3 Bucket 
    storage = aws_provider.get_storage("my-s3-bucket")
    assert storage.exists("my/cloud/object")
    storage.download("my/cloud/object", "/my/local/file")
    
    # Fetch an AWS Secret
    secret = aws_provider.get_secret("my-secret").plain
    print(f"Here's a cloud secret: {secret}")

Or with Azure yet using the same interface::

    from datalake import ServiceDiscovery

    azure_provider = ServiceDiscovery("azure")
    
    # Use a BlobStorage 
    storage = azure_provider.get_storage("myaccount.mycontainer")
    assert storage.exists("my/cloud/object")
    storage.download("my/cloud/object", "/my/local/file")
    
    # Fetch an KeyVault Secret
    secret = azure_provider.get_secret("my-secret").plain
    print(f"Here's a cloud secret: {secret}")

Storage
-------

The Storage interface makes abstraction of a cloud object store

- **AWS S3**: the name of the storage is the name of the bucket
- **Azure BlobStorage**: the name of the storage is the concatenation of the StorageAccount name and the container name separated with a dot ``.`` For example ``mystorageaccount.mycontainer``
- **Google Cloud Storage**: the name of the storage is the name of the bucket
- **locally** a storage is mocked with a folder

See :py:obj:`datalake.interface.IStorage` for all available methods

Secrets
-------

The Secrets interface makes abstraction of a cloud secret store

- **AWS Secret Manager**: the name of the secret is the same as in Secret Manager
- **Azure KeyVault**: the name of the secret is the concatenation of the KeyVault name and the secret name separated with a dot ``.`` For example ``mykeyvault.mysecret``
- **Google Secret Manager**: the name of the secret is the same as in Secret Manager
- **locally** a secret is mocked with a file in ``.secrets/`` folder

See :py:obj:`datalake.interface.ISecret` for all available methods
