from logging import getLogger
import pendulum
from time import perf_counter_ns
from datalake.interface import IMonitor
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class Measurement:  # pragma: no cover
    def __init__(self, name, start_time=None):
        """Represents a point of measurement consisting in a starting time, a set of measures and a set of labels

        Args:
            name (str): the name for the measurement
            start_time (time): the time when measurement started.
                Defaults to current UTC time.
        """
        self._name = name
        self._start = start_time if start_time is not None else pendulum.now("UTC")
        self._labels = {}
        self._measures = {"file_count": 1}
        self.reset_chrono()

    def __str__(self):
        return f"Metric '{self.name}' started at {self.start_time} with labels {self.labels} and measures {self.measures}"

    @property
    def name(self):
        """The name of the measurement"""
        return self._name

    @property
    def start_time(self):
        """The reference starting time for the measurement"""
        return self._start

    @start_time.setter
    def start_time(self, start_time):
        self._start = start_time

    @property
    def labels(self):
        """The ``dict`` of labels attached with the measurment"""
        return self._labels

    @labels.setter
    def labels(self, labels):
        if not isinstance(labels, dict):
            raise ValueError("Labels must be a key/value map")
        self._labels = labels

    @property
    def measures(self):
        """The ``dict`` of measure values. Defaults to ``{"file_count": 1}``"""
        return self._measures

    @measures.setter
    def measures(self, measures):
        if not isinstance(measures, dict):
            raise ValueError("Measures must be a key/value map")
        self._measures = measures

    def add_measure(self, key, value):
        """Appends a single measure

        Args:
            key (str): the measure name
            value (double): the measure value
        """
        self._measures[key] = value

    def add_measures(self, measures):
        """Appends a batch of measures

        Args:
            measures (dict): a key pair map of measure names and values
        """
        self._measures.update(measures)

    def add_label(self, key, value):
        """Appends a single label

        Args:
            key (str): the label name
            value (str): the label value
        """
        self._labels[key] = value

    def add_labels(self, labels):
        """Appends a batch of labels

        Args:
            labels (dict): a key pair map of label names and values
        """
        self._labels.update(labels)

    def reset_chrono(self):
        """Resets the counter used for evaluating elapsed time"""
        self._chrono = perf_counter_ns()

    def read_chrono(self):
        """Returns the elapsed time since last reset or since initialization"""
        return perf_counter_ns() - self._chrono


class NoMonitor(IMonitor):  # pragma: no cover
    def __init__(self, quiet=True, *args, **kwargs):
        """A quiet monitoring implementation used for disabling monitoring or for development/testing

        Args:
            quiet (bool): Whether metrics are logged or not
        """
        self._quiet = quiet

    def push(self, metric):
        """Writes the metric in the logger if **quiet** if set to ``False``"""
        if not self._quiet:
            logger = getLogger(__name__).info(metric)


class InfluxMonitor(IMonitor):  # pragma: no cover
    """Monitoring with InfluxDB OSS 2.x"""

    def __init__(self, url, token, org, bucket, *args, **kwargs):
        self._url = url
        self._token = token
        self._org = org
        self._bucket = bucket

    def push(self, metric):
        with InfluxDBClient(self._url, token=self._token, org=self._org) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)

            point = Point(metric.name)
            point.time(metric.start_time, WritePrecision.NS)
            for label, value in metric.labels.items():
                point.tag(label, value)
            for field, value in metric.measures.items():
                point.field(field, value)
            write_api.write(self._bucket, self._org, point)
