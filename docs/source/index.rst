Welcome to mpcq's documentation!
================================

``mpcq`` is a powerful Python client library for querying and analyzing Minor Planet Center (MPC) data through Google BigQuery. This package provides efficient access to a BigQuery instance of the Small Bodies Node (SBN) replica of the MPC's Small Bodies Node database, maintained by the Asteroid Institute.

Features
--------

- **BigQuery Integration**: Direct access to a complete replica of the MPC database through Google BigQuery
- **Efficient Queries**: Optimized query patterns for common asteroid data access patterns
- **Rich Data Access**: Query observations, orbits, submission history, and more
- **Cross-Matching**: Tools for matching observations and finding duplicates
- **ADES Support**: Integration with ADES format for modern asteroid data exchange

Data Structures
-------------

The ``mpcq`` package uses `adam-core <https://github.com/B612-Asteroid-Institute/adam_core>`_ data structures, which are built on top of `Quivr <https://github.com/B612-Asteroid-Institute/quivr>`_. Quivr provides strongly-typed tables backed by Apache Arrow for efficient memory usage and fast operations. All query results are returned as Quivr tables with predefined schemas:

- ``MPCObservations``: Contains observation data with columns like ``obstime``, ``ra``, ``dec``, etc.
- ``MPCOrbits``: Contains orbital elements with columns like ``a``, ``e``, ``i``, etc.
- ``MPCSubmissionHistory``: Contains submission history with columns like ``submission_time``, ``num_obs``, etc.
- ``MPCPrimaryObjects``: Contains object identification data linking different designations.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   bigquery_dataset
   api_reference
   examples
   contributing
   changelog

Getting Started
--------------

To get started with ``mpcq``, check out the :doc:`quickstart` guide.

BigQuery Dataset
---------------

The Asteroid Institute maintains a BigQuery replica of the Minor Planet Center's Small Bodies Node database. This dataset is publicly accessible through Google Cloud Platform. For more information about accessing and using the dataset, see :doc:`bigquery_dataset`.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search` 