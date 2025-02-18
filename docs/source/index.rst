Welcome to mpcq's documentation!
================================

``mpcq`` is a powerful Python client library for querying and analyzing Minor Planet Center (MPC) data, made available through the Small Bodies Node (SBN).
The data is hosted on Google BigQuery and maintained by the `Asteroid Institute <https://b612.ai>`_.

You can find the source code on `GitHub <https://github.com/B612-Asteroid-Institute/mpcq>`_ and the package on `PyPI <https://pypi.org/project/mpcq/>`_.

Features
--------

- **BigQuery Integration**: Quickly analyze MPC data without having to maintain your own replica.
- **Efficient Queries**: Optimized query patterns for common asteroid data access patterns
- **Rich Data Access**: Query observations, orbits, submission history, and more
- **Cross-Matching**: Tools for matching observations and finding duplicates

Data Structures
-------------

The ``mpcq`` package uses `adam-core <https://github.com/B612-Asteroid-Institute/adam_core>`_ data structures, which are built on top of `Quivr <https://github.com/B612-Asteroid-Institute/quivr>`_. Quivr provides strongly-typed tables backed by Apache Arrow for efficient memory usage and fast operations. All query results are returned as Quivr tables with predefined schemas:

- ``MPCObservations``: Contains observation data with columns like ``obstime``, ``ra``, ``dec``, etc.
- ``MPCOrbits``: Contains orbital elements with columns like ``a``, ``e``, ``i``, etc.
- ``MPCSubmissionHistory``: Contains submission history with columns like ``submission_time``, ``num_obs``, etc.
- ``MPCPrimaryObjects``: Contains object identification data linking different designations.

All of the data structures can easily be converted to pandas DataFrames.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   bigquery_dataset
   api_reference
   examples
   community
   contributing
   changelog

Getting Started
--------------

To get started with ``mpcq``, check out the :doc:`quickstart` guide.

BigQuery Dataset
---------------

The Asteroid Institute maintains a BigQuery replica of the Minor Planet Center's Small Bodies Node database. This dataset is publicly accessible through Google Cloud Platform. For more information about accessing and using the dataset, see :doc:`bigquery_dataset`.

Community
--------

Join our community! We have an active `mailing list <https://groups.io/g/adam-users/>`_ and Slack workspace where you can get help, share ideas, and contribute to the project. See our :doc:`community` page for more details on how to get involved.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search` 