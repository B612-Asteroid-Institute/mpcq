Installation
============

Requirements
-----------

The latest published ``mpcq`` 0.4.0 supports Python 3.11–3.12. This unreleased
source tree validates Python 3.11–3.14 (``>=3.11,<3.15``) against public
``adam-core`` 0.8 wheels in one uniform compatibility matrix. A future MPCQ
release and version alignment remain separately authorized work. The package has
the following core dependencies:

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

All four source-tree compatibility rows install the released public
``adam-core==0.8.0`` wheel and apply the same wheel, provenance, test, coverage,
typing, documentation, lock, and clean-tree checks. Each row also installs the
hash-pinned frozen runtime and development lock into an isolated environment
using public binary artifacts only. This evidence does not alter the immutable
published MPCQ 0.4.0 artifacts. While the release-readiness variable is unset,
the publication job is intentionally skipped; this is a fail-closed green skip
rather than a red workflow failure.

Google Cloud Setup
----------------

To use ``mpcq``, you'll need to:

1. Create a Google Cloud Platform account if you don't have one
2. Create a new project or select an existing one
3. Enable the BigQuery API for your project
4. Subscribe to the MPC datasets through Analytics Hub:

   a. Visit the `Main MPC Dataset <https://console.cloud.google.com/bigquery/analytics-hub/exchanges/projects/492788363398/locations/us/dataExchanges/asteroid_institute_mpc_replica_1950545e4f4/listings/asteroid_institute_mpc_replica_1950549970f>`_ listing and subscribe
   b. Note the dataset ID from your subscription

5. Set up authentication:

  An easy way is to use Application Default Credentials:

   .. code-block:: bash

       gcloud auth application-default login

Using the Client
--------------

After setting up authentication and subscribing to the dataset, you can initialize the client:

.. code-block:: python

    from mpcq.client import BigQueryMPCClient

    client = BigQueryMPCClient(dataset_id="your_subscribed_main_dataset_id")

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
