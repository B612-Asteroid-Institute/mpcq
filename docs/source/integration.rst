Integration Dataset and Tests
=============================

This project includes optional BigQuery integration tests that exercise live queries
against a small curated dataset. These tests are off by default and require explicit
opt-in via environment variables.

Curated Integration Dataset
---------------------------

The integration dataset lives in your GCP project under:

- ``moeyens-thor-dev.mpcq_it`` (main tables)

Content (populated from production):

- ``public_obs_sbn``: complete observation rows (all columns) for four real objects, one per station:
  - X05 → ``2025 MW47``
  - I41 → ``2025 PR1``
  - T05 → ``1948 AD``
  - T08 → ``1999 XK100``
  Identification expansion is applied, so observations include primary and secondary designations, and any numbered mapping via ``permid``.

- ``public_mpc_orbits``: the most recent orbit row per primary designation corresponding to the four objects.

- ``public_current_identifications`` and ``public_numbered_identifications``: rows linked to the selected primaries.

- ``public_primary_objects``: the corresponding primary object rows.

- ``public_obs_sbn`` is queried directly for cross-match tests using bounded station/time windows and spatial distance checks.

Running Integration Tests
-------------------------

Integration tests are marked with the ``integration`` pytest marker and are skipped unless explicitly enabled.

Environment variables:

- ``MPCQ_RUN_IT=1``: enable integration tests
- ``MPCQ_IT_DATASET``: BigQuery dataset id for main tables (default: ``moeyens-thor-dev.mpcq_it``)
- ``MPCQ_IT_MAX_BYTES``: per-job bytes cap (default: ``2000000000`` i.e., 2 GB)
- ``MPCQ_IT_BASE_PROVIDS``: comma-separated list of base designations to test (default: ``2025 MW47,2025 PR1,1948 AD,1999 XK100``)

Example:

.. code-block:: bash

   export MPCQ_RUN_IT=1
   export MPCQ_IT_DATASET=moeyens-thor-dev.mpcq_it
   pytest -m integration -q

What is Tested
--------------

- ``query_observations`` default behavior with all columns
- Column selection via the ``columns`` parameter
- Optional ``where`` filters including case-insensitive string operators and comparisons
- ``provids=None`` behavior requiring a ``limit``
- ``query_orbits`` returns the most recent orbit rows with constructed ``epoch``
- ``query_primary_objects`` presence
- ``cross_match_observations`` against the canonical observations table with partition-pruned filters

Notes
-----

- To keep costs low, the test fixture wraps BigQuery job configuration with ``maximum_bytes_billed`` and enables query caching by default.
- These tests require valid Google Cloud authentication in your environment.

