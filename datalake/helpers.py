import csv
import decimal
import os
import pendulum
import re
from tempfile import mkstemp
from babel.core import Locale

try:
    import pandas

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

STANDARD_DATE_FORMAT = "YYYY-MM-DD"
STANDARD_TIME_FORMAT = "HH:mm:ss.SSSZZ"
STANDARD_DATETIME_FORMAT = "YYYY-MM-DDTHH:mm:ss.SSSZZ"


def _to_decimal(x, lang):
    if type(x) in (int, float):
        return x
    l = Locale.parse(lang)
    separator = l.number_symbols["decimal"]
    group = l.number_symbols["group"]

    if re.fullmatch(r"\s+", group) is not None:
        group = " "

    try:
        return decimal.Decimal(x.replace(group, "").replace(separator, "."))
    except decimal.InvalidOperation:
        raise ValueError(f"Unable to cast number {x} in locale '{lang}'")


def _to_date(string, format):
    return pendulum.from_format(string, format)


def cast_integer(x, lang="en_US"):
    """Cast a string as an integer according to a locale

    Args:
        x (str): a value to cast
        lang (str): the locale used to interpret the string

    Returns:
        the value casted as ``int``
    """
    return int(_to_decimal(x, lang))


def cast_float(x, lang="en_US"):
    """Cast a string as an decimal according to a locale

    Args:
        x (str): a value to cast
        lang (str): the locale used to interpret the string.

    Returns:
        the value casted as ``float``
    """
    return float(_to_decimal(x, lang))


def cast_date(d, formats=[STANDARD_DATE_FORMAT]):
    """Cast a string as an date according to a set of formats strings

    Args:
        d (str): a value to cast
        formats (list(str)): a set of formats used to try to cast the string as a date. See also the `Supported formats`_

    Returns:
        the value formatted with `ISO 8601`_ date format

    .. _Supported formats:
       https://pendulum.eustace.io/docs/#tokens
    .. _ISO 8601:
       https://www.iso.org/iso-8601-date-and-time-format.html
    """
    for f in formats:
        try:
            return _to_date(d, f).format(STANDARD_DATE_FORMAT)
        except ValueError:
            continue
    raise ValueError(f"Wrong date format '{d}'")


def cast_time(d, formats=[STANDARD_TIME_FORMAT]):
    """Cast a string as an time according to a set of formats strings

    Args:
        d (str): a value to cast
        formats (list(str)): a set of formats used to try to cast the string as a time. See also the `Supported formats`_

    Returns:
        the value formatted with `ISO 8601`_ time format

    .. _Supported formats:
       https://pendulum.eustace.io/docs/#tokens
    .. _ISO 8601:
       https://www.iso.org/iso-8601-date-and-time-format.html
    """
    for f in formats:
        try:
            return _to_date(d, f).format(STANDARD_TIME_FORMAT)
        except ValueError:
            continue
    raise ValueError(f"Wrong time format '{d}'")


def cast_datetime(d, formats=[STANDARD_DATETIME_FORMAT]):
    """Cast a string as an datetime according to a set of formats strings

    Args:
        d (str): a value to cast
        formats (list(str)): a set of formats used to try to cast the string as a datetime. See also the `Supported formats`_

    Returns:
        the value formatted with `ISO 8601`_ datetime format

    .. _Supported formats:
       https://pendulum.eustace.io/docs/#tokens
    .. _ISO 8601:
       https://www.iso.org/iso-8601-date-and-time-format.html
    """
    for f in formats:
        try:
            return _to_date(d, f).format(STANDARD_DATETIME_FORMAT)
        except ValueError:
            continue
    raise ValueError(f"Wrong datetime format '{d}'")


class StandardDialect(csv.Dialect):
    """CSV format according to `RFC 4180`_

    .. _RFC 4180:
       https://datatracker.ietf.org/doc/html/rfc4180
    """

    delimiter = ","
    quotechar = '"'
    escapechar = None
    doublequote = True
    lineterminator = "\n"
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace = False
    strict = True


class DatasetBuilder:
    def __init__(self, datalake, key, path=None, lang="en_US", date_formats=None, ciphered=False):
        """Creates a new CSV file according to a Datalake's standards

        Args:
            datalake (Datalake): a datalake instance
            key (str): the catalog entry identifier for which a dataset is created
            path (str): the path to a local file to create. Default to a auto-generated temp file
            lang (str): a locale to use for casting numbers
            date_formats (list(str)): a list of datetime formats to use for casting date and times. Defaults to iso8601 formats.
            ciphered (bool): Indicates whether the dataset has pseudonymized values or not. Defaults to ``False``

        Example:
            building a dataset with dicts::

                with DatasetBuilder(dlk, "my-entry") as dsb:
                    for i in range(10):
                        dsb.add_dict({
                            "id": i,
                            "column_s" : "lorem ipsum",
                            "column_i": 1234.56,
                            "column_d": "2022-04-01",
                        })
                dlk.upload(dsb.path, "storename", "my-entry")
        """
        self._datalake = datalake
        self._catalog_key = key
        self._catalog_entry = datalake.get_entry(key)
        self._typing = [item["type"] for item in self._catalog_entry["columns"]]
        self._header = [item["name"] for item in self._catalog_entry["columns"]]
        self._managed_path = path is None
        self._path = path if not self._managed_path else self._new_temp_file()
        self._lang = lang
        self._date_formats = (
            date_formats
            if date_formats is not None
            else [STANDARD_DATE_FORMAT, STANDARD_TIME_FORMAT, STANDARD_DATETIME_FORMAT]
        )
        self._ciphered = []
        for item in self._catalog_entry["columns"]:
            if "gdpr" not in item or "pii" not in item["gdpr"]:
                self._ciphered.append(False)
            else:
                self._ciphered.append(ciphered and item["gdpr"]["pii"])
        self._row_count = 0

    def __del__(self):
        if self._managed_path:
            os.remove(self._path)

    def __enter__(self):
        self._file = open(self._path, "w", encoding="utf-8", newline="")
        self._writer = csv.writer(self._file, self._datalake.csv_dialect)
        self._writer.writerow(self._header)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._file.close()
        return False

    def _new_temp_file(self):
        (temp_file, temp_path) = mkstemp(prefix=f"datalake-dataset_", suffix=".csv")
        os.close(temp_file)
        return temp_path

    @property
    def path(self):
        """The path to the local file"""
        return self._path

    @property
    def row_count(self):
        """The number of rows appended in the dataset"""
        return self._row_count

    def new_dict(self):
        """Returns an empty row as ``dict``"""
        return {name: None for name in self._header}

    def _add(self, row):
        self._writer.writerow(row)
        self._row_count += 1

    def add_dict(self, row):
        """Appends a row in the dataset

        Args:
            row (dict): a row in key/value pairs
        """
        if row.keys() ^ set(self._header):
            raise ValueError(f"Row has not the expected column count {len(row.keys())}/{len(self._header)}")
        seq = []
        for idx in range(len(self._header)):
            seq.append(row[self._header[idx]])
        self.add_sequence(seq)

    def add_sequence(self, row):
        """Appends a row in the dataset

        Args:
            row (list): a row sequence of values
        """
        if len(row) != len(self._typing):
            raise ValueError(f"Row has not the expected column count {len(row)}/{len(self._typing)}")
        typed_row = []
        for idx in range(len(row)):
            typed_value = row[idx]
            if not self._ciphered[idx]:
                if isinstance(typed_value, str):
                    typed_value = re.sub(r"\s", " ", typed_value).strip()
                if typed_value is not None and typed_value != "":
                    if self._typing[idx] == "date":
                        typed_value = cast_date(typed_value, self._date_formats)
                    elif self._typing[idx] == "time":
                        typed_value = cast_time(typed_value, self._date_formats)
                    elif self._typing[idx] == "datetime":
                        typed_value = cast_datetime(typed_value, self._date_formats)
                    elif self._typing[idx] in ("number", "decimal"):
                        typed_value = cast_float(typed_value, self._lang)
                    elif self._typing[idx] == "integer":
                        typed_value = cast_integer(typed_value, self._lang)
            typed_row.append(typed_value)
        self._add(typed_row)


class DatasetReader:
    def __init__(self, datalake, store, key, path_params=None, ciphered=False):
        """Reads a CSV dataset from a store.

        No file is downloaded and data is streamed when using the iterators.

        Args:
            datalake (Datalake): a datalake instance
            store (str): the name of the store to read the dataset from
            key (str): the catalog entry identifier for which a dataset is read
            path_params (dict): the entry path placeholders used to find a specific dataset
            ciphered (bool): Indicates whether the dataset has pseudonymized values or not. Defaults to ``False``

        Example:
            Count the number of rows in a dataset::

                dsr = DatasetReader(dlk, "storename", "my-entry")
                count = 0
                for item in dsr.iter_list():
                    count += 1
                print(f"Found {count} rows")
        """
        self._datalake = datalake
        self._storage, self._path = self._datalake.get_entry_path_resolved(store, key, path_params, strict=True)
        self._catalog_key = key
        self._catalog_entry = datalake.get_entry(key)
        self._typing = [item["type"] for item in self._catalog_entry["columns"]]
        self._header = [item["name"] for item in self._catalog_entry["columns"]]
        self._ciphered = []
        for item in self._catalog_entry["columns"]:
            if "gdpr" not in item or "pii" not in item["gdpr"]:
                self._ciphered.append(False)
            else:
                self._ciphered.append(ciphered and item["gdpr"]["pii"])

    def iter_list(self):
        """Returns an iterator of ``list`` for each row"""
        reader = csv.reader(self._storage.stream(self._path), self._datalake.csv_dialect)
        row_count = 0
        for row in reader:
            row_count += 1
            if row_count <= 1:
                continue
            typed_row = []
            for idx in range(len(row)):
                typed_value = row[idx]
                if not self._ciphered[idx]:
                    if typed_value is not None and typed_value != "":
                        if self._typing[idx] in ("number", "decimal"):
                            typed_value = cast_float(typed_value)
                        elif self._typing[idx] == "integer":
                            typed_value = cast_integer(typed_value)
                typed_row.append(typed_value)
            yield typed_row

    def iter_dict(self):
        """Returns an iterator of ``dict`` for each row"""
        for row in self.iter_list():
            yield {self._header[idx]: value for idx, value in enumerate(row)}

    def dataframe(self):
        """Returns a pandas DataFrame

        Raises:
            RuntimeError: if pandas package is not installed
        """
        if not PANDAS_AVAILABLE:  # pragma: no cover
            raise RuntimeError("pandas package is missing")

        return pandas.DataFrame(self.iter_list(), columns=self._header)
