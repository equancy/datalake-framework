import importlib
import requests
import csv
from datalake.exceptions import DatalakeError, EntryNotFound, StoreNotFound


class DatalakeDialect(csv.Dialect):
    delimiter = ","
    quotechar = '"'
    escapechar = None
    doublequote = True
    lineterminator = "\n"
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace = False
    strict = True


class Datalake:
    """
    Main class for dealing with datacatalog and storage
    """

    def __init__(self, catalog_url):
        self._catalog_url = catalog_url
        config = self._call_catalog("configuration")

        # Configure Cloud provider
        self._provider = config["provider"]
        if self._provider == "aws":  # pragma: no cover
            self._provider_module = importlib.import_module("datalake.provider.aws")
        elif self._provider == "gcp":  # pragma: no cover
            self._provider_module = importlib.import_module("datalake.provider.gcp")
        elif self._provider == "local":
            self._provider_module = importlib.import_module("datalake.provider.local")
        else:  # pragma: no cover
            raise DatalakeError(f"Invalid datalake provider found: {self._provider}")

        # Configure CSV dialect
        self._dialect = DatalakeDialect()
        self._dialect.delimiter = config["csv_format"]["delimiter"]
        self._dialect.lineterminator = config["csv_format"]["line_break"]
        self._dialect.quotechar = config["csv_format"]["quote_char"]
        self._dialect.doublequote = config["csv_format"]["double_quote"]
        escape_char = config["csv_format"]["escape_char"]
        self._dialect.escapechar = None if escape_char == "" else escape_char

    def _call_catalog(self, endpoint, params=None):
        response = requests.get(f"{self._catalog_url}/{endpoint}", params=params)
        response.raise_for_status()
        return response.json()

    @property
    def provider(self):
        return self._provider

    @property
    def csv_dialect(self):
        return self._dialect

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

    def get_entry_path(self, key, path_params=None):
        """
        Builds a path for the specified entry with the given parameters
        """
        try:
            res = self._call_catalog(f"catalog/storage/{key}", path_params)
            return res["prefix"]
        except requests.exceptions.HTTPError as http_error:
            if http_error.response.status_code == 404:
                raise EntryNotFound(f"Entry '{key}' does not exist")
            raise  # pragma: no cover

    def get_entry_path_resolved(self, store, key, path_params={}):
        """
        Returns the resolved path for the specified entry with the given parameters and a storage for the specifed store
        """
        path = self.get_entry_path(key, path_params)
        bucket, resolved, _ = self.resolve_path(store, path)
        return self.get_storage(bucket), resolved
