Quickstart Guide
==============

This guide will help you get started with ``mpcq`` quickly. We'll cover basic setup and common usage patterns.

Basic Setup
----------

First, make sure you have ``mpcq`` installed and your Google Cloud credentials configured (see :doc:`installation`).

Subscribe to the Dataset
---------------------

Before you can use ``mpcq``, you need to subscribe to the MPC dataset through Google Cloud's Analytics Hub:

1. Visit the `Main MPC Dataset <https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/492788363398/locations/us/dataExchanges/asteroid_institute_mpc_replica_1950545e4f4/listings/asteroid_institute_mpc_replica_1950549970f>`_ listing
2. Click "Subscribe"
3. Visit the `Clustered Views Dataset <https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/492788363398/locations/us/dataExchanges/asteroid_institute_mpc_replica_1950545e4f4/listings/asteroid_institute_mpc_replica_views_195054bbe98>`_ listing
4. Click "Subscribe"
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

Query Observations
---------------

.. code-block:: python

    # Query observations - returns MPCObservations (Quivr table)
    observations = client.query_observations(["2013 RR165"])
    
    # Access data directly from the table
    print(f"Number of observations: {len(observations)}")
    print(f"First observation time: {observations.obstime[0]}")
    print(f"Observatories: {observations.stn.unique()}")

Working with Orbits
----------------

.. code-block:: python

    # Get orbit information - returns MPCOrbits (Quivr table)
    orbits = client.query_orbits(["2013 RR165"])
    
    # Access orbital elements directly
    print(f"Semi-major axis: {orbits.a}")
    print(f"Eccentricity: {orbits.e}")
    print(f"Inclination: {orbits.i}")

Submission History
---------------

.. code-block:: python

    # Get submission history - returns MPCSubmissionHistory (Quivr table)
    history = client.query_submission_history(["2013 RR165"])
    
    # Work with the data
    print(f"Number of submissions: {len(history)}")
    for submission in history:
        print(f"Submission {submission.submission_id}: {submission.num_obs} observations")
        print(f"Arc length: {submission.arc_length} days")

Cross-Matching Observations
------------------------

.. code-block:: python

    from adam_core.observations import ADESObservations

    # Cross-match with ADES observations
    matched = client.cross_match_observations(
        ades_observations,
        obstime_tolerance_seconds=30,
        arcseconds_tolerance=2.0
    )
    
    # Access matched data
    print(f"Found {len(matched)} matches")
    print(f"Average separation: {matched.separation_arcseconds.mean():.2f} arcsec")

Finding Duplicates
---------------

Find potential duplicate observations:

.. code-block:: python

    # Find duplicates for an object
    duplicates = client.find_duplicates(
        "2013 RR165",
        obstime_tolerance_seconds=30,
        arcseconds_tolerance=2.0
    )

Next Steps
---------

- Learn more about the :doc:`bigquery_dataset`
- Check out detailed :doc:`examples`
- Read the complete :doc:`api_reference` 