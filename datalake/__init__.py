import importlib
import requests


class Datalake:
    def __init__(self, catalog_url):
        self._catalog_url = catalog_url
        response = requests.get(f"{catalog_url}/configuration")
        self._configuration = response.json()
        self._provider = self._configuration["provider"]

        if self._provider == "aws":
            self._provider_module = importlib.import_module(
                "datalake.provider.aws"
            )
        else:
            self._provider_module = importlib.import_module(
                "datalake.provider.gcp"
            )

    @property
    def provider(self):
        return self._provider

    def get_storage(self, bucket):
        return self._provider_module.Storage(bucket)
