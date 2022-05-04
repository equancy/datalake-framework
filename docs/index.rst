Datalake Framework
==================

.. toctree::
   :maxdepth: 4
   :hidden:

   cloud-resources
   monitoring
   api/index

This python framework provides Data Engineering features for managing and organizing datasets in a Cloud datalake.

- Datacatalog driven operations
- Cloud storage abstraction.
- Monitoring

Getting started
---------------

Install the framework with **PIP**

.. code-block:: shell

    pip install datalake-framework


The main class in the framework is :py:obj:`datalake.Datalake`. It gets its configuration with a ``dict``

:catalog_url: the URL for the Datacatalog API
:monitoring: see :doc:`monitoring`

The Datacatalog API provides most of the parameters like the cloud provider and storage identifiers.

For example::

   from datalake import Datalake

   config = {
      "catalog_url": "http://catalog.datalake.svc:8080",
      "monitoring": {
         "class": "NoMonitor",
         "params": {
            "quiet": False
         }
      }
   }
   dlk = Datalake(config)

   # Fetch the dataset specs for a catalog entry
   my_entry = dlk.get_entry("my-entry")

   # Download a file from a storage bucket
   dlk.download("silver", my_entry["_key"], "/local/path/my-file.csv")


.. Indices and tables
.. ==================

.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`
