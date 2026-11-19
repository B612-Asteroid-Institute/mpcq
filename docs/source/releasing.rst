Releasing MPCQ
==============

MPCQ 0.5.0 is released from one reviewed commit and one accepted artifact set.
The release does not rebuild distributions after acceptance.

Prerequisites
-------------

The repository variable ``MPCQ_RELEASES_ENABLED`` must be ``true`` only for the
bounded release window. Before dispatch, create (or verify) the ``pypi`` GitHub
environment with at least one required reviewer and one custom deployment policy
allowing only the exact ``v0.5.0`` tag. Self-review may remain enabled, matching
the established Core and Assist production environments. Configure that
environment as the trusted publisher for the ``mpcq`` PyPI project. The publish
workflow checks the reviewer and tag policy before requesting an OIDC upload token.
Adam Assist 0.7.0 must already be public and verified before MPCQ publication
begins; MPCQ does not depend on Adam Assist.

Candidate
---------

After the release-preparation pull request has passed the Python 3.11–3.14
matrix and merged normally, dispatch ``Release candidate artifacts`` from the
exact ``main`` commit. The workflow exports ``PDM_BUILD_SCM_VERSION=0.5.0`` only
inside a ``git archive`` snapshot without SCM metadata. It builds one universal
wheel and one source distribution, verifies their metadata and generated runtime
version, runs the installed-wheel suite against public Adam Core 0.8, and stores
the exact bytes with a SHA-256 manifest bound to the full commit SHA.

The source distribution is not byte-reproducible across separate candidate runs;
archive metadata may change even when source content does not. Publication and
every recovery or retry must therefore reuse the same accepted candidate run ID
and its manifest. Never rebuild or dispatch a replacement candidate as a fallback
for an already-started publication.

Publication
-----------

Review the candidate run and its two checksums. Create the immutable annotated
``v0.5.0`` tag at that same commit, then dispatch ``Publish accepted MPCQ
artifacts`` from the tag. Supply the successful candidate run ID, full release
SHA, version ``0.5.0``, and the exact confirmation requested by the workflow.
The protected job checks the tag and candidate-run provenance, downloads the
accepted bytes, and uploads only files not already present on PyPI.

PyPI filenames are immutable. A retry is safe only when every existing filename
has the accepted SHA-256 digest; a conflicting digest, unexpected file, yanked
file, wrong tag, wrong candidate run, or wrong confirmation stops publication.
After upload, the workflow waits for PyPI and verifies that the complete public
release is exactly the accepted wheel and source distribution. Create the GitHub
Release only after this verification succeeds, then return
``MPCQ_RELEASES_ENABLED`` to its disabled state.
