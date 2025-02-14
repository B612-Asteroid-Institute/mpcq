API Reference
============

This section provides detailed documentation for the ``mpcq`` API.

Data Structures
------------

The ``mpcq`` package uses strongly-typed tables built with `Quivr <https://github.com/B612-Asteroid-Institute/quivr>`_ and `adam-core <https://github.com/B612-Asteroid-Institute/adam_core>`_. These tables provide efficient memory usage, type safety, and fast operations through Apache Arrow.

MPCObservations
^^^^^^^^^^^^^

.. autoclass:: mpcq.observations.MPCObservations
   :members:
   :undoc-members:
   :show-inheritance:

Contains observation data with columns:
   - ``obstime``: Observation timestamp (Timestamp)
   - ``ra``, ``dec``: Position in degrees (Float64)
   - ``rmsra``, ``rmsdec``: Position uncertainties (Float64)
   - ``mag``: Magnitude (Float64)
   - ``band``: Filter band (String)
   - ``stn``: Observatory code (String)
   - And more...

CrossMatchedMPCObservations
^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: mpcq.observations.CrossMatchedMPCObservations
   :members:
   :undoc-members:
   :show-inheritance:

Contains cross-matched observations with additional columns:
   - ``separation_arcseconds``: Angular separation between matches (Float64)
   - ``separation_seconds``: Time difference between matches (Float64)
   - ``mpc_observations``: Nested MPCObservations table

MPCOrbits
^^^^^^^^

.. autoclass:: mpcq.orbits.MPCOrbits
   :members:
   :undoc-members:
   :show-inheritance:

Contains orbital elements with columns:
   - ``epoch``: Epoch of orbital elements (Timestamp)
   - ``a``: Semi-major axis in AU (Float64)
   - ``e``: Eccentricity (Float64)
   - ``i``: Inclination in degrees (Float64)
   - ``om``: Longitude of ascending node in degrees (Float64)
   - ``w``: Argument of perihelion in degrees (Float64)
   - ``ma``: Mean anomaly in degrees (Float64)

MPCPrimaryObjects
^^^^^^^^^^^^^^^

.. autoclass:: mpcq.orbits.MPCPrimaryObjects
   :members:
   :undoc-members:
   :show-inheritance:

Contains object identification data with columns:
   - ``primary_designation``: Primary designation (String)
   - ``provid``: Provisional designation (String)
   - ``permid``: Permanent identifier (String)
   - ``created_at``, ``updated_at``: Timestamps for record updates

Client Classes
------------

BigQueryMPCClient
^^^^^^^^^^^^^^^

.. autoclass:: mpcq.client.BigQueryMPCClient
   :members:
   :undoc-members:
   :show-inheritance:

The main client for interacting with the MPC BigQuery dataset. All query methods return strongly-typed Quivr tables.

Submission Classes
---------------

MPCSubmissionResults
^^^^^^^^^^^^^^^^^

.. autoclass:: mpcq.submissions.MPCSubmissionResults
   :members:
   :undoc-members:
   :show-inheritance:

MPCSubmissionHistory
^^^^^^^^^^^^^^^^^

.. autoclass:: mpcq.submissions.MPCSubmissionHistory
   :members:
   :undoc-members:
   :show-inheritance:
