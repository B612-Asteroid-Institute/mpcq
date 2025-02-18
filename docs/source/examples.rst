Examples
========

This section provides detailed examples of common tasks using ``mpcq``.

Basic Object Queries
-----------------

Query observations for a single object:

.. code-block:: python

    from mpcq.client import BigQueryMPCClient
    import pyarrow.compute as pc

    client = BigQueryMPCClient(
        dataset_id="your_subscribed_main_dataset_id",
        views_dataset_id="your_subscribed_views_dataset_id"
    )
    
    # Get observations for one or more objects
    observations = client.query_observations(["2013 RR165"])
    
    # Basic analysis
    print(f"Number of observations: {len(observations)}")
    print(f"Date range: {observations.obstime.min().mjd()} to {observations.obstime.max().mjd()}")
    print(f"Observatories: {pc.unique(observations.stn)}")


MPC Orbit Queries
--------------

Fetch one or more orbits from the MPC:

.. code-block:: python

    from mpcq.client import BigQueryMPCClient

    client = BigQueryMPCClient(
        dataset_id="your_subscribed_main_dataset_id",
        views_dataset_id="your_subscribed_views_dataset_id"
    )

    # Fetch orbits for one or more objects
    orbits = client.query_orbits(["2013 RR165", "2024 YR4"])
    
    # Basic analysis
    print(f"Number of orbits: {len(orbits)}")
    
    # You can view the data and get it as a pandas DataFrame
    print(orbits.to_dataframe())


Cross-Matching with Observations
------------------------------

Cross-match your observations with the MPC database:

.. code-block:: python

    from adam_core.observations import ADESObservations, ADES_string_to_tables
    from adam_core.time import Timestamp
    input_observations = ADESObservations.from_kwargs(
        # These are the only required columns for the cross-match
        obsSubID=["1234567890", "1234567891"],
        obsTime=Timestamp.from_iso8601(['2011-01-30T11:15:25.920', '2011-01-30T11:37:22.656'], scale="utc"),
        ra=[123.884679, 123.880767],
        dec=[19.820047, 19.820603],
        stn=["F51", "F51"],
        astCat=["Gaia2", "Gaia2"],
        mode=["CCD", "CCD"],
    )

    client = BigQueryMPCClient(
        dataset_id="your_subscribed_main_dataset_id",
        views_dataset_id="your_subscribed_views_dataset_id"
    )

    # Now you can cross-match the observations
    matched = client.cross_match_observations(input_observations)

    # Uses your provided obsSubID as the input_id for the cross-match
    print(matched.to_dataframe())

    # See the residuals to the matches
    print(matched.separation_arcsec, matched.separation_seconds)


Get the status of a Submission
----------------------------

Given one or more submission IDs, get the status of the corresponding observations.
Similar to WAMO, this can be useful for tracking ones submissions.

.. code-block:: python

    # Get submission status
    observation_status = client.query_submission_info(["2022-05-23T23:16:35.633_0000EfpX"])

    print(observation_status.to_dataframe())

Working with Submission History
---------------------------

Get a history of all submissions for one or more objects. This can be useful
for breaking down which submissions contributed to arc length of number of observations.

.. code-block:: python

    # Get submission history
    history = client.query_submission_history(["2013 RR165"])

    print(history.to_dataframe())


