Branch Evaluation (2026-04-04)
==============================

Scope
-----

Compared three branches:

- ``codex/update-replica-partitioning-clustering`` (``13fae00``)
- ``codex/remove-views-dataset-id`` (``0586ac7``)
- ``codex/combine-replica-no-views-performance`` (new combined branch)

Evaluation focused on:

- functional correctness against live BigQuery datasets
- dry-run bytes processed for representative queries
- live runtime on the curated integration dataset


Keep / Reject Matrix
--------------------

Keep from ``update-replica``:

- expanded replicated schemas in ``MPCObservations`` / ``MPCOrbits``
- dynamic ``columns`` + ``where`` + ``limit`` support in query APIs
- timestamp conversion updates for newer ``adam_core``
- integration test scaffold + developer tools additions

Reject from ``update-replica``:

- hard dependency on ``views_dataset_id`` / ``public_obs_sbn_clustered`` (fails on production replica)
- untyped query parameters (causes ``Invalid query parameter type`` in live runs)
- selecting JSON columns in ``SELECT DISTINCT`` without conversion

Keep from ``remove-views``:

- no-views client contract (single dataset id)
- SQL input normalization and escaping helpers
- cross-match time/station bounded query shape

Modify while combining:

- keep no-views architecture, but use ``SAFE_CAST(... AS FLOAT64)`` in geospatial calls because ``ra``/``dec`` are currently string-typed in live tables
- convert structured/json replica columns with ``TO_JSON_STRING`` before materializing into quivr string columns
- add typed BigQuery parameters for all ``where`` operators


Dry-Run Results (Bytes Processed)
---------------------------------

Dataset: ``moeyens-thor-dev.mpcq_it``

+----------------------------+------------+------------+----------+
| Query                      | update     | remove     | combined |
+============================+============+============+==========+
| query_observations         | error      | 3,077,367  | 9,464,881|
+----------------------------+------------+------------+----------+
| query_orbits               | error      | 851        | 6,177    |
+----------------------------+------------+------------+----------+
| cross_match_observations   | 1,157,292  | 2,839,175  | 2,839,175|
+----------------------------+------------+------------+----------+
| find_duplicates            | 2,839,175  | 2,839,175  | 2,839,175|
+----------------------------+------------+------------+----------+

Dataset: ``moeyens-thor-dev.mpc_sbn_aurora``

+----------------------------+------------+---------------+---------------+
| Query                      | update     | remove        | combined      |
+============================+============+===============+===============+
| query_observations         | error      | 86,218,557,997| 231,605,454,588 |
+----------------------------+------------+---------------+---------------+
| query_orbits               | error      | 299,083,012   | 2,358,084,840 |
+----------------------------+------------+---------------+---------------+
| cross_match_observations   | error      | 106,968,493   | 106,968,493   |
+----------------------------+------------+---------------+---------------+
| find_duplicates            | 77,649,984,383 | 77,649,984,383 | 77,649,984,383 |
+----------------------------+------------+---------------+---------------+

Notes:

- ``update`` cross-match fails on production due missing ``..._views.public_obs_sbn_clustered``.
- ``update`` observation/orbit queries fail on JSON columns used with ``SELECT DISTINCT``.
- combined default queries intentionally scan more bytes because they project many more replica columns.


Column Pruning Effect (Combined Branch)
---------------------------------------

On ``moeyens-thor-dev.mpc_sbn_aurora``:

- ``query_observations`` default: 231,605,454,588 bytes
- ``query_observations`` subset (``obsid``, ``stn``, ``obstime`` + where): 26,339,477,472 bytes
- Reduction: ~88.6%

On ``query_orbits``:

- default: 2,358,071,368 bytes
- subset (``provid``, ``epoch``, ``q``, ``e``, ``i``): 128,885,956 bytes
- Reduction: ~94.5%


Live Runtime Results (mpcq_it, No Cache, 2GB Max Bytes)
--------------------------------------------------------

+----------------------------+----------------+----------------+----------------+
| Query                      | update         | remove         | combined       |
+============================+================+================+================+
| query_observations_default | error          | 4.167 s        | 5.555 s        |
+----------------------------+----------------+----------------+----------------+
| query_observations_subset  | error          | n/a            | 2.670 s        |
+----------------------------+----------------+----------------+----------------+
| query_orbits_default       | error          | 2.078 s        | 2.431 s        |
+----------------------------+----------------+----------------+----------------+
| cross_match_observations   | 4.575 s        | 2.422 s        | 2.521 s        |
+----------------------------+----------------+----------------+----------------+


Decision
--------

``codex/combine-replica-no-views-performance`` is the recommended PR branch.

It combines the replica/schema/API improvements with the no-views operational model, and fixes the live-query regressions present in ``update``.

