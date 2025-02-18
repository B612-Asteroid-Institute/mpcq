Quickstart Guide
==============

This guide will help you get started with ``mpcq`` quickly. We'll cover basic setup and common usage patterns.

Basic Setup
----------

First, make sure you have ``mpcq`` installed and your Google Cloud credentials configured (see :doc:`installation`).

Subscribe to the Dataset
---------------------

Before you can use ``mpcq``, you need to subscribe to the MPC dataset through Google Cloud's Analytics Hub:

1. Visit the `Main MPC Dataset <https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/492788363398/locations/us/dataExchanges/asteroid_institute_mpc_replica_1950545e4f4/listings/asteroid_institute_mpc_replica_1950549970f>`__ listing
2. Click "Subscribe" and create a linked dataset in your project
3. Visit the `Clustered Views Dataset <https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/492788363398/locations/us/dataExchanges/asteroid_institute_mpc_replica_1950545e4f4/listings/asteroid_institute_mpc_replica_views_195054bbe98>`_ listing
4. Click "Subscribe" and create a linked dataset in your project
5. Note the dataset IDs from your subscriptions

Initialize the Client
-------------------

.. code-block:: python

    from mpcq.client import BigQueryMPCClient

    # Initialize the client with your subscribed dataset IDs
    client = BigQueryMPCClient(
        dataset_id="your_subscribed_main_dataset_id",
        views_dataset_id="your_subscribed_views_dataset_id"
    )

.. warning::
   The MPC dataset in BigQuery is large, containing millions of observations and orbits. Running queries will incur Google Cloud Platform billing charges based on the amount of data scanned.

   There is a small free allowance of 1TB analysis credits per month in BigQuery, but this will quickly be consumed with queries against the large obervations database.

   For more information on BigQuery billing, see the `BigQuery documentation <https://cloud.google.com/bigquery/pricing>`_.


Query Observations
---------------

.. code-block:: python

    import pyarrow.compute as pc

    # Query observations - returns MPCObservations (Quivr table)
    observations = client.query_observations(["2013 RR165"])
    
    # Access data directly from the table
    print(f"Number of observations: {len(observations)}")
    print(f"First observation time: {observations.obstime[0]}")
    print(f"Observatories: {pc.unique(observations.stn)}")

    # Or convert to a pandas DataFrame
    print(observations.to_dataframe())


Working with Orbits
----------------

.. code-block:: python

    # Get orbit information - returns MPCOrbits (Quivr table)
    orbits = client.query_orbits(["2013 RR165"])
    
    # Access orbital elements directly
    print(f"Semi-major axis: {orbits.a}")
    print(f"Eccentricity: {orbits.e}")
    print(f"Inclination: {orbits.i}")


    # You can quickly convert to an adam_core.orbits.Orbit object,
    # to be used with the adam_core propagators and other tools.
    adam_core_orbits = orbits.orbits()


A Note on Quivr
-------------

The ``mpcq`` package primarily uses `Quivr <https://github.com/B612-Asteroid-Institute/quivr>`_ Tables for data structures. ``quivr`` tables are similar to pandas DataFrames, but provide:

- Strict schemas and type safety
- Composability
- Efficient memory usage, backed by Apache Arrow
- Optimized serialization/deserialization to Parquet

For example, ``MPCObservations``, ``MPCOrbits``, ``MPCSubmissionHistory``, and ``ADESObservations`` are all ``quivr`` Tables with well-defined schemas. For interoperability with pandas, all ``quivr`` tables have a ``to_dataframe()`` method.

You can view detailed ``quivr`` docs `here <https://quivr.readthedocs.io/en/stable/>`_.


Next Steps
---------

- Learn more about the :doc:`bigquery_dataset`
- Check out detailed :doc:`examples`
- Read the complete :doc:`api_reference` 