"""
Fixtures for testing the experimental submission manager.
"""

import os
import tempfile
from datetime import datetime, timezone
from typing import Dict
from unittest.mock import Mock

import numpy as np
import pyarrow as pa
import pytest
import sqlalchemy as sq
from adam_core.observations import SourceCatalog
from adam_core.observations.ades import (
    ObsContext,
    ObservatoryObsContext,
    SoftwareObsContext,
    SubmitterObsContext,
    TelescopeObsContext,
)
from adam_core.time import Timestamp

from mpcq.submissions.manager import SubmissionManager
from mpcq.submissions.mpc import MPCSubmissionClient
from mpcq.submissions.types import (
    AssociationCandidateMembers,
    AssociationCandidates,
    DiscoveryCandidateMembers,
    DiscoveryCandidates,
    SubmissionMembers,
    Submissions,
    Submitter,
    Submitters,
)
from mpcq.submissions.wamo import WAMOResults


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def temp_submission_dir(temp_dir):
    """Create a temporary submission manager directory."""
    manager_dir = os.path.join(temp_dir, "test_manager")
    os.makedirs(manager_dir, exist_ok=True)
    return manager_dir


@pytest.fixture
def sample_submitter():
    """Create a sample submitter."""
    return Submitter(
        first_name="Test",
        last_name="User",
        email="test@example.com",
        institution="Test Institution",
        id=1,
    )


@pytest.fixture
def sample_submitters_table():
    """Create a sample Submitters table."""
    return Submitters.from_kwargs(
        id=[1, 2],
        first_name=["Test", "Another"],
        last_name=["User", "Submitter"],
        email=["test@example.com", "another@example.com"],
        institution=["Test Institution", "Another Institution"],
        created_at=[
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        ],
    )


@pytest.fixture
def sample_source_catalog():
    """Create a sample SourceCatalog with observations."""
    n_obs = 20  # Increased to support both discovery and association submissions

    # Create timestamps
    times = Timestamp.from_mjd(
        pa.array([60000.0 + i * 0.1 for i in range(n_obs)]), scale="utc"
    )

    # Create SourceCatalog directly with minimal required fields
    return SourceCatalog.from_kwargs(
        id=[f"obs_{i:03d}" for i in range(n_obs)],
        catalog_id=pa.array(["test_catalog"] * n_obs),
        time=times,
        ra=pa.array([10.0 + i * 0.01 for i in range(n_obs)]),
        dec=pa.array([20.0 + i * 0.01 for i in range(n_obs)]),
        observatory_code=pa.array(["X05"] * n_obs),
        # Optional but needed for ADES conversion
        ra_sigma=pa.array([0.1] * n_obs),
        dec_sigma=pa.array([0.1] * n_obs),
        radec_corr=pa.array([0.0] * n_obs),
        mag=pa.array([20.0] * n_obs),
        mag_sigma=pa.array([0.1] * n_obs),
        filter=pa.array(["r"] * n_obs),
        exposure_id=[f"exp_{i//2:03d}" for i in range(n_obs)],
        exposure_duration=pa.array([30.0] * n_obs),
        exposure_seeing=pa.array([1.5] * n_obs),
        astrometric_catalog=pa.array(["Gaia DR3"] * n_obs),
        photometric_catalog=pa.array(["PS1"] * n_obs),
        snr=pa.array([50.0] * n_obs),
    )


@pytest.fixture
def sample_discovery_candidates():
    """Create sample discovery candidates."""
    return DiscoveryCandidates.from_kwargs(trksub=["trk_001", "trk_002", "trk_003"])


@pytest.fixture
def sample_discovery_members(sample_source_catalog):
    """Create sample discovery candidate members."""
    obs_ids = sample_source_catalog.id.to_pylist()
    return DiscoveryCandidateMembers.from_kwargs(
        trksub=["trk_001"] * 3 + ["trk_002"] * 4 + ["trk_003"] * 3,
        obssubid=obs_ids[:10],
    )


@pytest.fixture
def sample_association_candidates():
    """Create sample association candidates."""
    return AssociationCandidates.from_kwargs(
        trksub=["trk_101", "trk_102"],
        permid=[None, None],
        provid=["2023 AA1", "2023 AB2"],
    )


@pytest.fixture
def sample_association_members(sample_source_catalog):
    """Create sample association candidate members."""
    obs_ids = sample_source_catalog.id.to_pylist()
    # Use second half of observations to avoid conflicts with discovery members
    return AssociationCandidateMembers.from_kwargs(
        trksub=["trk_101"] * 5 + ["trk_102"] * 5,
        obssubid=obs_ids[10:20],
    )


@pytest.fixture
def sample_obscontexts():
    """Create sample ObsContext dictionary."""
    return {
        "X05": ObsContext(
            observatory=ObservatoryObsContext(mpcCode="X05", name="Test Observatory"),
            submitter=SubmitterObsContext(
                name="Test Submitter", institution="Test Institution"
            ),
            measurers=["Test Measurer"],
            telescope=TelescopeObsContext(
                design="Reflector",
                aperture=1.0,
                detector="CCD",
                name="1.0-m Test Telescope",
            ),
            observers=["Test Observer"],
            software=SoftwareObsContext(astrometry="test_software v1.0"),
        )
    }


@pytest.fixture
def mock_mpc_submission_client():
    """Create a mock MPC submission client."""
    client = Mock(spec=MPCSubmissionClient)

    # Mock submit_ades to return a submission ID and timestamp
    client.submit_ades.return_value = (
        "test_submission_id_12345678901234567890",
        datetime.now(timezone.utc),
    )

    # Mock query_wamo to return sample results
    def mock_query_wamo(requested_values, timeout=120):
        results = []
        for val in requested_values:
            results.extend(
                [
                    {
                        val: [
                            {
                                "submission_id": val,
                                "submission_block_id": f"{val}_block",
                                "obsid": f"mpc_{i:03d}",
                                "obssubid": f"obs_{i:03d}",
                                "status": "P",
                                "ref": "",
                                "iau_desig": "2023 AA",
                                "input_type": "ADES",
                                "obs80": "",
                                "status_decoded": "Published",
                            }
                            for i in range(3)
                        ]
                    }
                ]
            )

        return WAMOResults.from_json(
            {
                "found": results,
                "not_found": [],
                "malformed": [],
            }
        )

    client.query_wamo.side_effect = mock_query_wamo

    return client


@pytest.fixture
def mock_mpc_sbn_client():
    """Create a mock MPC SBN client."""
    from mpcq.client import MPCClient

    client = Mock(spec=MPCClient)

    # Mock query_submission_results
    def mock_query_submission_results(submission_ids):
        from mpcq.observations import MPCObservations

        # Return empty observations table for now
        return MPCObservations.empty()

    client.query_submission_results.side_effect = mock_query_submission_results

    return client


@pytest.fixture
def empty_manager(temp_submission_dir):
    """Create a fresh SubmissionManager in a temp directory."""
    return SubmissionManager.create(temp_submission_dir)


@pytest.fixture
def manager_with_submitter(empty_manager, sample_submitter):
    """Create a manager with a submitter already added."""
    empty_manager.add_submitter(sample_submitter)
    empty_manager._submitter = sample_submitter
    return empty_manager


@pytest.fixture
def sample_submissions():
    """Create sample Submissions table."""
    return Submissions.from_kwargs(
        id=["20241016_d12345", "20241016_a67890"],
        mpc_submission_id=[None, None],
        submitter_id=[1, 1],
        type=["discovery", "association"],
        linkages=[3, 2],
        observations=[10, 10],
        first_observation_mjd_utc=[60000.0, 60000.5],
        last_observation_mjd_utc=[60001.0, 60001.5],
        created_at=[datetime.now(timezone.utc), datetime.now(timezone.utc)],
        submitted_at=[None, None],
        file_path=["/path/to/file1.psv", "/path/to/file2.psv"],
        file_md5=["abc123", "def456"],
        comment=["Test discovery", "Test association"],
        error=[None, None],
    )


@pytest.fixture
def sample_submission_members():
    """Create sample SubmissionMembers table."""
    return SubmissionMembers.from_kwargs(
        submission_id=["20241016_d12345"] * 10,
        trksub=["trk_001"] * 3 + ["trk_002"] * 4 + ["trk_003"] * 3,
        permid=[None] * 10,
        provid=[None] * 10,
        obssubid=[f"obs_{i:03d}" for i in range(10)],
        mpc_obsid=[None] * 10,
        mpc_status=[None] * 10,
        mpc_permid=[None] * 10,
        mpc_provid=[None] * 10,
        updated_at=[None] * 10,
    )


@pytest.fixture
def sample_wamo_results():
    """Create sample WAMO results."""
    submission_id = "test_submission_id_12345678901234567890"
    return WAMOResults.from_json(
        {
            "found": [
                {
                    submission_id: [
                        {
                            "submission_id": submission_id,
                            "submission_block_id": f"{submission_id}_block",
                            "obsid": f"mpc_obs_{i:03d}",
                            "obssubid": f"obs_{i:03d}",
                            "status": "P",
                            "ref": "",
                            "iau_desig": "2023 AA",
                            "input_type": "ADES",
                            "obs80": "",
                            "status_decoded": "Published",
                        }
                        for i in range(10)
                    ]
                }
            ],
            "not_found": [],
            "malformed": [],
        }
    )
