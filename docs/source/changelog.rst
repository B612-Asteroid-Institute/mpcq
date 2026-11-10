Changelog
=========

All notable changes to ``mpcq`` will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[Unreleased]
-----------

Added
^^^^^
- CPython 3.13 and 3.14 source compatibility, with an exact CPython 3.14
  ``adam-core`` candidate wheel lane, retained Python 3.11–3.12 coverage, and
  newly added Python 3.13 coverage.
- Governance for the source-tested Python matrix, registry-only Core 0.7 lock,
  exact Core source ref, installed-wheel test and coverage provenance, and
  fail-closed publication boundary.
- Comprehensive documentation with ReadTheDocs integration
- Detailed examples and usage guides
- BigQuery dataset documentation

Changed
^^^^^^^
- Replaced the stale moving ``adam_core`` Git-branch dependency with the released
  ``adam-core>=0.7.0,<0.8.0`` registry range for Python 3.11–3.13. CPython 3.14
  publication remains blocked pending separately authorized successor Core and
  MPCQ version/range alignment.
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