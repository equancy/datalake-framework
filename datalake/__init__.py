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
        else: # pragma: no cover
            raise DatalakeError(f"Invalid datalake provider found: {self._provider}")

        # Configure CSV dialect
        self._dialect = DatalakeDialect()
        self._dialect.delimiter = config["csv_format"]["delimiter"]
        self._dialect.lineterminator = config["csv_format"]["line_break"]
        self._dialect.quotechar = config["csv_format"]["quote_char"]
        self._dialect.doublequote = config["csv_format"]["double_quote"]
        escape_char = config["csv_format"]["escape_char"]
        self._dialect.escapechar = None if escape_char == "" else escape_char

    def _call_catalog(self, endpoint):
        response = requests.get(f"{self._catalog_url}/{endpoint}")
        response.raise_for_status()
        return response.json()

    @property
    def provider(self):
        return self._provider

    @property
    def csv_dialect(self):
        return self._dialect

    def get_storage(self, bucket):
        return self._provider_module.Storage(bucket)

    def get_catalog(self, key):
        try:
            return self._call_catalog(f"catalog/entry/{key}")
        except requests.exceptions.HTTPError as http_error:
            if http_error.response.status_code == 404:
                raise EntryNotFound(f"Entry '{key}' does not exist")
            raise  # pragma: no cover
    
    def resolve_path(self, store, path):
        try:
            res = self._call_catalog(f"storage/{store}/{path}")
            return res["bucket"], res["path"], res["uri"]
        except requests.exceptions.HTTPError as http_error:
            if http_error.response.status_code == 404:
                raise StoreNotFound(f"Store '{store}' does not exist")
            raise  # pragma: no cover
