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

  An easy way is to use Application Default Credentials:

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

Queries to the BigQuery dataset will be billed according to your Google Cloud Platform account's BigQuery pricing. 
BigQuery offers a free tier, but the limits are too low for users that plan to be running more than tens of queries per month.


For more details on pricing and cost management, see :ref:`pricing-and-free-tier`.

Development Installation
----------------------

For development, you can install ``mpcq`` from source.
We use `pdm <https://pdm.fming.dev/latest/>`_ to manage dependencies and tooling.

.. code-block:: bash

    git clone https://github.com/B612-Asteroid-Institute/mpcq.git
    cd mpcq
    pdm install -G dev
