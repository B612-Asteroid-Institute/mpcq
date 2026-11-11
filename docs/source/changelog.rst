Changelog
=========

All notable changes to ``mpcq`` will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[Unreleased]
-----------

Added
^^^^^
- CPython 3.13 and 3.14 source compatibility in one uniform Python 3.11–3.14
  matrix using released public ``adam-core==0.8.0`` wheels.
- An authoritative whole-document SHA-256 pin for the Python workflow, with
  parsed-YAML structural checks retained as readable diagnostics; plus a fully
  refreshed binary-only runtime/development lock check on every row,
  installed-wheel coverage provenance, and the publication boundary.
- Comprehensive documentation with ReadTheDocs integration
- Detailed examples and usage guides
- BigQuery dataset documentation

Changed
^^^^^^^
- Replaced the stale moving ``adam_core`` Git-branch dependency with released
  ``adam-core>=0.8.0,<0.9.0`` public artifacts across Python 3.11–3.14. MPCQ
  publication and version alignment remain separately authorized work.
- Runtime/science behavior is unchanged; query, schema, uncertainty, and
  non-gravitational parameter mapping code is untouched.
- While the release-readiness variable is unset, the publication job is
  intentionally skipped. This is a fail-closed green skip, not a red failure.

[0.1.0] - 2024-02-14
------------------

Added
^^^^^
- Initial release of ``mpcq``
- BigQuery client implementation
- Support for querying observations, orbits, and submission history
- Cross-matching functionality
- Duplicate detection
- Basic utility functions for data conversion
- Integration with ADAM Core for ADES support 