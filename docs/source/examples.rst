Examples
========

This section provides detailed examples of common tasks using ``mpcq``.

Basic Object Queries
-----------------

Query observations for a single object:

.. code-block:: python

    from mpcq.client import BigQueryMPCClient
    from mpcq.utils import observations_to_dataframe

    client = BigQueryMPCClient()
    
    # Get observations for a specific object
    observations = client.query_observations(["2013 RR165"])
    df = observations_to_dataframe(observations)
    
    # Basic analysis
    print(f"Number of observations: {len(df)}")
    print(f"Date range: {df['obstime'].min()} to {df['obstime'].max()}")
    print(f"Observatories: {df['stn'].unique()}")

Working with Multiple Objects
-------------------------

Query and compare multiple objects:

.. code-block:: python

    # List of objects to query
    objects = ["2013 RR165", "2015 BP519", "2012 VP113"]
    
    # Get observations for all objects
    all_observations = []
    for obj in objects:
        obs = client.query_observations([obj])
        df = observations_to_dataframe(obs)
        df['object'] = obj
        all_observations.append(df)
    
    # Combine into single DataFrame
    import pandas as pd
    combined_df = pd.concat(all_observations)
    
    # Analysis by object
    for obj in objects:
        obj_data = combined_df[combined_df['object'] == obj]
        print(f"\nObject: {obj}")
        print(f"Observations: {len(obj_data)}")
        print(f"Time span: {obj_data['obstime'].min()} to {obj_data['obstime'].max()}")

Orbital Evolution
--------------

Track how an object's orbit changes over time:

.. code-block:: python

    # Get orbit information
    orbits = client.query_orbits(["2013 RR165"])
    
    # Convert to DataFrame for analysis
    import numpy as np
    
    orbit_data = []
    for orbit in orbits:
        orbit_data.append({
            'epoch': orbit.epoch,
            'a': orbit.a,
            'e': orbit.e,
            'i': orbit.i,
            'q': orbit.a * (1 - orbit.e),  # perihelion distance
            'Q': orbit.a * (1 + orbit.e)   # aphelion distance
        })
    
    orbit_df = pd.DataFrame(orbit_data)
    orbit_df = orbit_df.sort_values('epoch')
    
    # Plot orbital evolution
    import matplotlib.pyplot as plt
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
    
    ax1.plot(orbit_df['epoch'], orbit_df['a'], 'b-', label='Semi-major axis')
    ax1.plot(orbit_df['epoch'], orbit_df['q'], 'g--', label='Perihelion')
    ax1.plot(orbit_df['epoch'], orbit_df['Q'], 'r--', label='Aphelion')
    ax1.set_ylabel('Distance (AU)')
    ax1.legend()
    
    ax2.plot(orbit_df['epoch'], orbit_df['i'], 'k-')
    ax2.set_ylabel('Inclination (deg)')
    ax2.set_xlabel('Epoch')
    
    plt.tight_layout()
    plt.show()

Finding Duplicate Observations
--------------------------

Identify and analyze potential duplicate observations:

.. code-block:: python

    # Find duplicates with default tolerances
    duplicates = client.find_duplicates("2013 RR165")
    
    # Convert to DataFrame
    dup_df = observations_to_dataframe(duplicates)
    
    # Group by submission ID to see patterns
    by_submission = dup_df.groupby('submission_id').agg({
        'obstime': 'count',
        'created_at': 'first'
    }).sort_values('created_at')
    
    print("Submissions with potential duplicates:")
    print(by_submission)

Cross-Matching with ADES Data
--------------------------

Cross-match your ADES observations with the MPC database:

.. code-block:: python

    from adam_core.observations import ADESObservations
    
    # Assuming you have ADES observations loaded
    matched = client.cross_match_observations(
        ades_observations,
        obstime_tolerance_seconds=30,
        arcseconds_tolerance=2.0
    )
    
    # Analyze matches
    match_df = observations_to_dataframe(matched)
    print(f"Total matches found: {len(match_df)}")
    
    # Look at position differences
    print("\nPosition difference statistics (arcseconds):")
    print(match_df['separation_arcsec'].describe())

Working with Submission History
---------------------------

Analyze an object's submission history:

.. code-block:: python

    # Get submission history
    history = client.query_submission_history(["2013 RR165"])
    
    # Convert to DataFrame
    from mpcq.utils import submissions_to_dataframe
    history_df = submissions_to_dataframe(history)
    
    # Sort by timestamp
    history_df = history_df.sort_values('timestamp')
    
    # Print submission timeline
    print("Submission timeline:")
    for _, row in history_df.iterrows():
        print(f"{row['timestamp']}: {row['num_observations']} observations")
    
    # Plot submission history
    plt.figure(figsize=(10, 5))
    plt.plot(history_df['timestamp'], history_df['num_observations'], 'bo-')
    plt.xlabel('Submission Date')
    plt.ylabel('Number of Observations')
    plt.title('Observation Submission History')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show() 