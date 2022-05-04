Monitoring
==========

Framework helps producing metrics for pipelines, use-cases etc.

Usage
-----

build a metric with :py:obj:`datalake.telemetry.Measurement`::

    from datalake.telemetry import Measurement

    process_metric = Measurement("my-custom-metric")
    process_metric.add_measure("file_size", storage.size(path))
    ...
    process_metric.add_measure("elapsed_time", round(process_metric.read_chrono() // 10**6))
    process_metric.add_label("status", "Success")

    
Publish the above metric with an implementation of :py:obj:`datalake.interface.IMonitor`::  

    from datalake import ServiceDiscovery

    services = ServiceDiscovery(CLOUD_PROVIDER, MONITORING_CONFIG)
    services.monitor.safe_push(process_metric)

Configuration
-------------

:py:obj:`datalake.ServiceDiscovery` works like a factory for finding the correct implementation for your monitoring.
The monitoring backend is customizable with a 

:class: a string indicating the class implementing of :py:obj:`datalake.interface.IMonitor`
:params: a dict of keyword arguments to pass to the class ``__init__``


For example, print the metrics on standard output::

    MONITORING_CONFIG = {
        "class": "NoMonitor",
        "params": {
            "quiet": False
        }
    }
    ServiceDiscovery("local", MONITORING_CONFIG).monitor.safe_push(process_metric)

Or, use a custom backend::

    MONITORING_CONFIG = {
        "class": "mypackage.MyMonitoring",
        "params": {
            "hostname": "my-backend",
            "port": 123,
        }
    }
    ServiceDiscovery("local", MONITORING_CONFIG).monitor.safe_push(process_metric)

The following implementations are included in the framework 

Disable Monitoring
^^^^^^^^^^^^^^^^^^

This class doesn't send the metrics to any backend.

**class** ``"NoMonitor"``

**parameters**

:quiet: if set to ``False`` the metric is printed on the standard output. Default is ``True``.

InfluxDB 2.x
^^^^^^^^^^^^

This class sends the metrics to an `InfluxDB`_ backend.

**class**: ``"InfluxMonitor"``

**parameters**:

:url: the URL of your InfluxDB instance
:token: the API token
:org: the parent influx organization
:bucket: the influx bucket to store the metric


Google Monitoring
^^^^^^^^^^^^^^^^^

This class sends the metrics to `Google Monitoring`_ backend.

**class** ``"datalake.provider.gcp.GoogleMonitor"``

**parameters**

:project_id: the target GCP project ID hosting the Google Monitoring metrics

.. _InfluxDB:
    https://docs.influxdata.com/influxdb/
.. _Google Monitoring:
    https://cloud.google.com/monitoring/
