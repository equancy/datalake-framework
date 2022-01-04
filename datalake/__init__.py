import inspect
from importlib import import_module
import requests
from datalake.helpers import StandardDialect, DatasetBuilder, DatasetReader
from datalake.interface import IMonitor
from datalake.exceptions import *


class Datalake:
    """
    Main class for dealing with datacatalog and storage
    """

    def __init__(self, config={}):
        if not isinstance(config, dict):
            raise BadConfiguration("Datalake configuration must be a dict")

        datalake_config = {
            "catalog_url": "http://localhost:8080",
            "monitoring": {
                "class": "NoMonitor",
                "params": {},
            },
        }
        datalake_config.update(config)
        self._catalog_url = datalake_config["catalog_url"]
        try:
            catalog_config = self._call_catalog("configuration")
        except Exception as e:
            raise BadConfiguration(f"catalog URL is invalid ({str(e)})")

        self._find_monitor_class(datalake_config["monitoring"])

        # Configure Cloud provider
        self._provider = catalog_config["provider"]
        if self._provider == "aws":  # pragma: no cover
            self._provider_module = import_module("datalake.provider.aws")
        elif self._provider == "gcp":  # pragma: no cover
            self._provider_module = import_module("datalake.provider.gcp")
        elif self._provider == "azure":  # pragma: no cover
            self._provider_module = import_module("datalake.provider.azure")
        elif self._provider == "local":
            self._provider_module = import_module("datalake.provider.local")
        else:  # pragma: no cover
            raise DatalakeError(f"Invalid datalake provider found: {self._provider}")

        # Configure CSV dialect
        self._dialect = StandardDialect()
        self._dialect.delimiter = catalog_config["csv_format"]["delimiter"]
        self._dialect.lineterminator = catalog_config["csv_format"]["line_break"]
        self._dialect.quotechar = catalog_config["csv_format"]["quote_char"]
        self._dialect.doublequote = catalog_config["csv_format"]["double_quote"]
        escape_char = catalog_config["csv_format"]["escape_char"]
        self._dialect.escapechar = None if escape_char == "" else escape_char

    def _call_catalog(self, endpoint, params=None):
        response = requests.get(f"{self._catalog_url}/{endpoint}", params=params)
        response.raise_for_status()
        return response.json()

    def _find_monitor_class(self, config):
        if not isinstance(config, dict):
            raise BadConfiguration("Monitoring configuration must be a dict")

        monitor_split = config["class"].split(".")
        if len(monitor_split) > 1:
            class_name = monitor_split[-1]
            module_name = ".".join(monitor_split[:-1])
        else:
            class_name = monitor_split[0]
            module_name = "datalake.telemetry"

        try:
            module = import_module(module_name)
        except ModuleNotFoundError:
            raise BadConfiguration(f"'{module_name}' Monitor module cannot be found")

        monitor_class = None
        for n, c in inspect.getmembers(module, inspect.isclass):
            if n.lower() == class_name.lower():
                monitor_class = c
                break
        if monitor_class is None:
            raise BadConfiguration(f"'{class_name}' Monitor class cannot be found in module {module_name}")
        if not issubclass(c, IMonitor):
            raise BadConfiguration(f"'{class_name}' Monitor class is not a subclass for IMonitor")

        try:
            self._monitor = c(**config["params"])
        except Exception as e:
            raise BadConfiguration(f"'{class_name}' Monitor class cannot be instanciated: {str(e)}")

    @property
    def provider(self):
        return self._provider

    @property
    def csv_dialect(self):
        return self._dialect

    @property
    def monitor(self):
        """
        Return the monitoring instance
        """
        return self._monitor

    def get_storage(self, bucket):
        """
        Return a storage from right provider
        """
        return self._provider_module.Storage(bucket)

    def get_entry(self, key):
        """
        Return the specified catalog entry
        """
        try:
            return self._call_catalog(f"catalog/entry/{key}")
        except requests.exceptions.HTTPError as http_error:
            if http_error.response.status_code == 404:
                raise EntryNotFound(f"Entry '{key}' does not exist")
            raise  # pragma: no cover

    def resolve_path(self, store, path):
        """
        Returns a tuple `(bucket, path, uri)` resolving the path in the specified store
        """
        try:
            res = self._call_catalog(f"storage/{store}/{path}")
            return res["bucket"], res["path"], res["uri"]
        except requests.exceptions.HTTPError as http_error:
            if http_error.response.status_code == 404:
                raise StoreNotFound(f"Store '{store}' does not exist")
            raise  # pragma: no cover

    def identify(self, path):
        """
        Returns a tuple with the catalog entry that matches the path and a `dict` with the path's placeholders
        """
        try:
            res = self._call_catalog(f"catalog/identify/{path}")
            if len(res) > 1:
                raise EntryNotFound(f"Multiple entry found for path '{path}'")
            return res[0]["entry"], res[0]["params"]
        except requests.exceptions.HTTPError as http_error:
            if http_error.response.status_code == 404:
                raise EntryNotFound(f"No entry found for path '{path}'")
            raise  # pragma: no cover

    def get_entry_path(self, key, path_params=None, strict=False):
        """
        Builds a path for the specified entry with the given parameters
        """
        try:
            res = self._call_catalog(f"catalog/storage/{key}", path_params)
            if strict and res["is_partial"]:
                raise ValueError(f"Missing parameters for entry '{key}'")
            return res["prefix"]
        except requests.exceptions.HTTPError as http_error:
            if http_error.response.status_code == 404:
                raise EntryNotFound(f"Entry '{key}' does not exist")
            raise  # pragma: no cover

    def get_entry_path_resolved(self, store, key, path_params=None, strict=False):
        """
        Returns the resolved path for the specified entry with the given parameters and a storage for the specifed store
        """
        path = self.get_entry_path(key, path_params, strict)
        bucket, resolved, _ = self.resolve_path(store, path)
        return self.get_storage(bucket), resolved

    def upload(
        self, filepath, store, key, path_params=None, content_type="text/plain", encoding="utf-8", metadata={}
    ):  # pragma: no cover
        """
        Uploads a local file in a store as the specified catalog entry
        """
        storage, path = self.get_entry_path_resolved(store, key, path_params, strict=True)
        storage.upload(filepath, path, content_type, encoding, metadata)
        return path

    def download(self, store, key, filepath, path_params=None):  # pragma: no cover
        """
        Downloads the specified catalog entry from a store to a local file
        """
        storage, path = self.get_entry_path_resolved(store, key, path_params, strict=True)
        storage.download(path, filepath)

    def list_entry_files(self, store, key, path_params=None):  # pragma: no cover
        """
        Returns the list of files in a store for the specified catalog entry
        """
        storage, path = self.get_entry_path_resolved(store, key, path_params, strict=False)
        return storage.keys_iterator(path)

    def new_dataset_builder(self, key, path=None, lang="en_US", date_formats=None, ciphered=False):
        return DatasetBuilder(self, key, path, lang, date_formats, ciphered)

    def new_dataset_reader(self, store, key, path_params=None):
        return DatasetReader(self, store, key, path_params)

    def get_secret(self, name):
        """
        Return a secret from right provider
        """
        return self._provider_module.Secret(bucket)
