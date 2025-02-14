Installation
============

This guide will help you install ``mpcq`` and set up your environment.

Requirements
----------

- Python 3.9 or later
- A Google Cloud Platform account
- Access to the MPC BigQuery dataset (see :doc:`quickstart`)

Installation Methods
-----------------

Using pip
^^^^^^^^

The recommended way to install ``mpcq`` is through pip:

.. code-block:: bash

    pip install mpcq

Development Installation
^^^^^^^^^^^^^^^^^^^^

For development, we use PDM to manage dependencies and the development environment:

1. Install PDM if you haven't already:

   .. code-block:: bash

       pip install pdm

2. Clone the repository:

   .. code-block:: bash

       git clone https://github.com/B612-Asteroid-Institute/mpcq.git
       cd mpcq

3. Install development dependencies:

   .. code-block:: bash

       pdm install -G dev

This will install all dependencies, including those needed for development, testing, and documentation.

Google Cloud Setup
---------------

1. Install the Google Cloud SDK
2. Authenticate with Google Cloud:

   .. code-block:: bash

       gcloud auth application-default login

3. Subscribe to the MPC dataset (see :doc:`quickstart`)

Verifying Installation
-------------------

To verify your installation:

.. code-block:: python

    from mpcq.client import BigQueryMPCClient

    # Should print the version number
    print(BigQueryMPCClient.version())

Troubleshooting
------------

Common Issues
^^^^^^^^^^^

1. Authentication errors:
   - Ensure you're logged in with ``gcloud auth application-default login``
   - Check your Google Cloud project has billing enabled

2. Import errors:
   - Verify Python version (3.9+)
   - Check if all dependencies are installed
   - Try reinstalling with ``pip install --force-reinstall mpcq``

Getting Help
^^^^^^^^^^

If you encounter issues:

1. Check the :doc:`quickstart` guide
2. Search existing GitHub issues
3. Open a new issue if needed
