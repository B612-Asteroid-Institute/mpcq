Installation
============

Requirements
-----------

``mpcq`` requires Python 3.11 or later (< 3.13). The package has the following core dependencies:

- ``google-cloud-bigquery``
- ``pyarrow``
- ``numpy``
- ``astropy``
- ``adam-core``

Installing mpcq
-------------

You can install ``mpcq`` using pip:

.. code-block:: bash

    pip install mpcq

Google Cloud Setup
----------------

To use ``mpcq``, you'll need to:

1. Create a Google Cloud Platform account if you don't have one
2. Create a new project or select an existing one
3. Enable the BigQuery API for your project
4. Subscribe to the MPC datasets through Analytics Hub:

   a. Visit the `Main MPC Dataset <https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/492788363398/locations/us/dataExchanges/asteroid_institute_mpc_replica_1950545e4f4/listings/asteroid_institute_mpc_replica_1950549970f>`_ listing and subscribe
   b. Visit the `Clustered Views Dataset <https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/492788363398/locations/us/dataExchanges/asteroid_institute_mpc_replica_1950545e4f4/listings/asteroid_institute_mpc_replica_views_195054bbe98>`_ listing and subscribe
   c. Note the dataset IDs from your subscriptions

5. Set up authentication:

   a. Create a service account and download the JSON key file
   b. Set the environment variable ``GOOGLE_APPLICATION_CREDENTIALS`` to point to your key file:

   .. code-block:: bash

       export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"

   c. Alternatively, you can use Application Default Credentials:

   .. code-block:: bash

       gcloud auth application-default login

Using the Client
--------------

After setting up authentication and subscribing to the datasets, you can initialize the client:

.. code-block:: python

    from mpcq.client import BigQueryMPCClient

    client = BigQueryMPCClient(
        dataset_id="your_subscribed_main_dataset_id",
        views_dataset_id="your_subscribed_views_dataset_id"
    )

Cost Considerations
-----------------

Queries to the BigQuery dataset will be billed according to your Google Cloud Platform account's BigQuery pricing. BigQuery offers a generous free tier:

- **Monthly Free Tier**:
    - 1 TB of query processing
    - 10 GB of active storage
    - 10 GB of long-term storage

Beyond the free tier, costs are based on:

- Query pricing: $5.00 per TB of data processed
- Storage pricing: $0.02 per GB per month for active storage

To manage costs effectively:

- Use the BigQuery Console to estimate query costs before running them
- Consider setting up billing alerts and quotas
- Use query optimization techniques:
    - Select specific columns instead of ``SELECT *``
    - Use ``LIMIT`` to test queries
    - Filter early in queries to reduce data processed
- Cache frequently accessed results locally

You can estimate query costs programmatically:

.. code-block:: python

    from google.cloud import bigquery

    # Configure a dry run
    job_config = bigquery.QueryJobConfig(dry_run=True)
    
    # Your query
    query = "SELECT * FROM `your_dataset.public_obs_sbn`"
    
    # Get bytes that would be processed
    query_job = client.query(query, job_config=job_config)
    bytes_processed = query_job.total_bytes_processed
    
    # Estimate cost ($5.00 per TB)
    estimated_cost_usd = (bytes_processed / 1e12) * 5.00

For more details on pricing and cost management, see :doc:`bigquery_dataset`.

Development Installation
----------------------

For development, you can install ``mpcq`` from source.
We use `pdm <https://pdm.fming.dev/latest/>`_ to manage the dependencies.

.. code-block:: bash

    git clone https://github.com/B612-Asteroid-Institute/mpcq.git
    cd mpcq
    pdm install -G dev
