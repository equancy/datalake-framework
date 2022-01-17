from logging import getLogger
import pendulum
from time import perf_counter_ns
from datalake.interface import IMonitor
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class Measurement:  # pragma: no cover
    """
    Represents an metric or measurement consisting in a starting time, a set of measures and a set of labels
    """

    def __init__(self, name, start_time=None):
        self._name = name
        self._start = start_time if start_time is not None else pendulum.now("UTC")
        self._labels = {}
        self._measures = {"file_count": 1}
        self.reset_chrono()

    def __str__(self):
        return f"Metric '{self.name}' started at {self.start_time} with labels {self.labels} and measures {self.measures}"

    @property
    def name(self):
        return self._name

    @property
    def start_time(self):
        return self._start

    @start_time.setter
    def start_time(self, start_time):
        self._start = start_time

    @property
    def labels(self):
        return self._labels

    @labels.setter
    def labels(self, labels):
        if not isinstance(labels, dict):
            raise ValueError("Labels must be a key/value map")
        self._labels = labels

    @property
    def measures(self):
        return self._measures

    @measures.setter
    def measures(self, measures):
        if not isinstance(measures, dict):
            raise ValueError("Measures must be a key/value map")
        self._measures = measures

    def add_measure(self, key, value):
        self._measures[key] = value

    def add_measures(self, measures):
        self._measures.update(measures)

    def add_label(self, key, value):
        self._labels[key] = value

    def add_labels(self, labels):
        self._labels.update(labels)

    def reset_chrono(self):
        self._chrono = perf_counter_ns()

    def read_chrono(self):
        return perf_counter_ns() - self._chrono


class NoMonitor(IMonitor):  # pragma: no cover
    """
    Disables monitoring
    """

    def __init__(self, quiet=True, *args, **kwargs):
        self._quiet = quiet

    def push(self, metric):
        if not self._quiet:
            logger = getLogger(__name__).info(metric)


class InfluxMonitor(IMonitor):  # pragma: no cover
    """
    Monitoring with InfluxDB OSS 2.x
    """

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
