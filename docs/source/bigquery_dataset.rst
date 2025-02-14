BigQuery Dataset
===============

The Asteroid Institute maintains a BigQuery instance of the Small Bodies Node (SBN) replica of the Minor Planet Center database. The SBN provides replication services for the MPC database (see https://sbnmpc.astro.umd.edu/MPC_database/replication-info.shtml), and the Asteroid Institute maintains a BigQuery instance of this replica.

The dataset is available through Google Cloud's Analytics Hub and requires subscription to two listings:

1. `Main MPC Dataset <https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/492788363398/locations/us/dataExchanges/asteroid_institute_mpc_replica_1950545e4f4/listings/asteroid_institute_mpc_replica_1950549970f>`_
2. `Clustered Views Dataset <https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/492788363398/locations/us/dataExchanges/asteroid_institute_mpc_replica_1950545e4f4/listings/asteroid_institute_mpc_replica_views_195054bbe98>`_

Dataset Scale and Storage
----------------------

The MPC dataset is substantial in size, which contributes to both its power and the importance of careful query optimization. For example, the primary observations table ``public_obs_sbn`` contains:

- **Row Count**: Over 479 million observations (479,055,827 rows)
- **Logical Size**: 181.44 GB of data
- **Physical Storage**: 
  - Current: 28.82 GB (compressed)
  - Total: 411.68 GB (including time travel and system snapshots)

This scale enables powerful analyses but also means that full table scans can quickly consume significant query resources. Understanding these numbers is crucial for:

1. **Query Performance**: The large row count means indexes and query optimization are essential
2. **Cost Management**: Scanning the full observations table processes ~181 GB of data (~$0.90 at standard pricing). However, you rarely need all columns.
3. **Time Travel**: The system maintains ~383 GB of time travel data for point-in-time analysis

Dataset Access
------------

To access the dataset:

1. Create a Google Cloud Platform account if you don't have one
2. Visit the Analytics Hub listings using the links above
3. Subscribe to both the main dataset and the views dataset
4. Note the dataset IDs from your subscriptions - you'll need these to initialize the client

After subscribing, you'll receive two dataset IDs that you'll use to initialize the ``BigQueryMPCClient``:

.. code-block:: python

    from mpcq.client import BigQueryMPCClient

    client = BigQueryMPCClient(
        dataset_id="your_subscribed_main_dataset_id",
        views_dataset_id="your_subscribed_views_dataset_id"
    )

Dataset Overview
--------------

- **Access**: Public, requires Google Cloud Platform account and Analytics Hub subscription
- **Billing**: Usage billed through your GCP account according to BigQuery pricing
- **Updates**: Regular updates from the MPC Small Bodies Node

Pricing and Free Tier
------------------

BigQuery offers a free tier and a pay-as-you-go pricing model:

**Free Tier (Monthly)**:
- 1 TB of query processing
- 10 GB of active storage

**Standard Pricing**:
- Query pricing: $5.00 per TB of data processed
- Storage pricing: $0.02 per GB per month for active storage

Cost Management
-------------

To manage BigQuery costs effectively:

1. **Query Optimization**:
   - Use column selection instead of ``SELECT *``
   - Add ``WHERE`` clauses early in your query
   - Test queries with ``LIMIT`` first
   - Use the clustered views when possible

2. **Cost Control**:
   - Set up billing alerts in Google Cloud Console
   - Use query quotas to prevent accidental large queries
   - Consider using `maximum_bytes_billed` in your queries


Key Tables
---------

public_obs_sbn
^^^^^^^^^^^^^

The primary observations table containing all asteroid observations:

.. code-block:: sql

    SELECT *
    FROM `moeyens-thor-dev.mpc_sbn_aurora.public_obs_sbn`
    WHERE provid = '2013 RR165'
    LIMIT 5

Key columns:
- ``obsid``: Unique observation identifier
- ``provid``: Provisional designation
- ``obstime``: Observation timestamp
- ``ra``, ``dec``: Position in degrees
- ``mag``: Magnitude
- ``band``: Filter band
- ``stn``: Observatory code
- ``submission_id``: Submission identifier

public_current_identifications
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Links between different designations for the same object:

.. code-block:: sql

    SELECT *
    FROM `moeyens-thor-dev.mpc_sbn_aurora.public_current_identifications`
    WHERE unpacked_secondary_provisional_designation = '2013 RR165'

Key columns:
- ``unpacked_primary_provisional_designation``
- ``unpacked_secondary_provisional_designation``
- ``permid``

public_numbered_identifications
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Information about numbered asteroids:

.. code-block:: sql

    SELECT *
    FROM `moeyens-thor-dev.mpc_sbn_aurora.public_numbered_identifications`
    WHERE permid = '12345'

Key columns:
- ``permid``: Permanent identifier
- ``unpacked_primary_provisional_designation``

public_orbits
^^^^^^^^^^^

Orbital elements for objects:

.. code-block:: sql

    SELECT *
    FROM `moeyens-thor-dev.mpc_sbn_aurora.public_orbits`
    WHERE provid = '2013 RR165'
    ORDER BY epoch DESC
    LIMIT 1

Key columns:
- ``provid``: Provisional designation
- ``epoch``: Epoch of orbital elements
- ``a``, ``e``, ``i``: Semi-major axis, eccentricity, inclination
- ``om``, ``w``, ``ma``: Longitude of ascending node, argument of perihelion, mean anomaly

Performance Optimization
---------------------

The dataset includes several performance optimizations:

1. **Clustered Views**:
   The ``public_obs_sbn_clustered`` view in the views dataset provides significant performance improvements. Here's a real-world comparison of the same query run against both tables:

   .. code-block:: sql

       -- Query: Count observations for specific observatories
       -- Version 1: Using main table
       SELECT stn, COUNT(obsid) 
       FROM `your_dataset.public_obs_sbn` 
       WHERE stn in ("W68", "T08", "T05", "M22") 
       GROUP BY stn;

       -- Version 2: Using clustered view
       SELECT stn, COUNT(obsid) 
       FROM `your_views_dataset.public_obs_sbn_clustered` 
       WHERE stn in ("W68", "T08", "T05", "M22") 
       GROUP BY stn;

   The clustered view typically provides:
   - 80%+ reduction in data processed
   - Significantly faster query execution
   - Lower query costs

2. **Query Best Practices**:
   - Use clustered views when possible for better performance
   - Filter on indexed columns when possible
   - Use ``LIMIT`` to test queries before running on full dataset
   - Consider partitioning large result sets

Example Queries
-------------

Find all observations of an object:

.. code-block:: sql

    SELECT 
        obstime,
        ra,
        dec,
        mag,
        band,
        stn
    FROM `moeyens-thor-dev.mpc_sbn_aurora.public_obs_sbn`
    WHERE provid = '2013 RR165'
    ORDER BY obstime ASC

Find objects with multiple designations:

.. code-block:: sql

    WITH object_ids AS (
        SELECT 
            unpacked_primary_provisional_designation,
            unpacked_secondary_provisional_designation,
            permid
        FROM `moeyens-thor-dev.mpc_sbn_aurora.public_current_identifications`
        WHERE unpacked_secondary_provisional_designation = '2013 RR165'
    )
    SELECT DISTINCT
        o.obstime,
        o.ra,
        o.dec,
        o.provid,
        i.unpacked_primary_provisional_designation
    FROM `moeyens-thor-dev.mpc_sbn_aurora.public_obs_sbn` o
    JOIN object_ids i
        ON o.provid = i.unpacked_secondary_provisional_designation
        OR o.provid = i.unpacked_primary_provisional_designation
    ORDER BY o.obstime ASC

Cost Management
-------------

To manage BigQuery costs effectively, it's important to understand the scale of the data:

1. **Query Cost Examples**:
   - Full scan of observations table (181.44 GB): ~$0.90
   - Scanning 10% of the table: ~$0.09
   - Monthly free tier (1 TB) could process the full table ~5.5 times

2. **Query Optimization Strategies**:
   - Use column selection instead of ``SELECT *``
   - Add ``WHERE`` clauses early in your query
   - Test queries with ``LIMIT`` first
   - Use the clustered views when possible
   - Consider caching frequently accessed results

3. **Cost Estimation**:
   
   .. code-block:: python

       from google.cloud import bigquery

       # Configure a dry run
       job_config = bigquery.QueryJobConfig(dry_run=True)
       
       # Example: full table scan
       query = "SELECT * FROM `your_dataset.public_obs_sbn`"
       query_job = client.query(query, job_config=job_config)
       bytes_processed = query_job.total_bytes_processed
       
       # Cost at $5.00 per TB
       estimated_cost_usd = (bytes_processed / 1e12) * 5.00
       
       print(f"This query will process {bytes_processed/1e9:.2f} GB")
       print(f"Estimated cost: ${estimated_cost_usd:.2f}")
