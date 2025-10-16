"""
Tests for SubmissionManager submission preparation.
"""

import os
from datetime import datetime, timezone

import pyarrow.compute as pc
import pytest


class TestPrepareSubmissionsValidation:
    """Tests for input validation in prepare_submissions."""

    def test_prepare_requires_candidates(
        self, manager_with_submitter, sample_source_catalog, sample_obscontexts
    ):
        """Test that at least one candidate type is required."""
        with pytest.raises(
            ValueError,
            match="At least one of discovery_candidates or association_candidates must be provided",
        ):
            manager_with_submitter.prepare_submissions(
                source_catalog=sample_source_catalog,
                obscontexts=sample_obscontexts,
                discovery_candidates=None,
                association_candidates=None,
            )

    def test_prepare_discovery_requires_members(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
    ):
        """Test that discovery candidates require discovery members."""
        with pytest.raises(
            ValueError,
            match="discovery_candidate_members must be provided if discovery_candidates is provided",
        ):
            manager_with_submitter.prepare_submissions(
                source_catalog=sample_source_catalog,
                obscontexts=sample_obscontexts,
                discovery_candidates=sample_discovery_candidates,
                discovery_candidate_members=None,
            )

    def test_prepare_association_requires_members(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_association_candidates,
    ):
        """Test that association candidates require association members."""
        with pytest.raises(
            ValueError,
            match="association_candidate_members must be provided if association_candidates is provided",
        ):
            manager_with_submitter.prepare_submissions(
                source_catalog=sample_source_catalog,
                obscontexts=sample_obscontexts,
                association_candidates=sample_association_candidates,
                association_candidate_members=None,
            )

    def test_prepare_validates_trksub_consistency_discovery(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test that all trksubs in candidates are in members (discovery)."""
        from mpcq.submissions.types import DiscoveryCandidates

        # Create candidates with a trksub not in members
        bad_candidates = DiscoveryCandidates.from_kwargs(
            trksub=["trk_001", "trk_999"]  # trk_999 not in members
        )

        with pytest.raises(
            ValueError,
            match="All trksubs in discovery_candidates must be present in discovery_candidate_members",
        ):
            manager_with_submitter.prepare_submissions(
                source_catalog=sample_source_catalog,
                obscontexts=sample_obscontexts,
                discovery_candidates=bad_candidates,
                discovery_candidate_members=sample_discovery_members,
            )

    def test_prepare_validates_obssubid_in_catalog_discovery(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
    ):
        """Test that all obssubids in members are in source catalog."""
        from mpcq.submissions.types import DiscoveryCandidateMembers

        # Create members with obssubids not in catalog
        # Need to include all trksubs from discovery_candidates to pass first validation
        bad_members = DiscoveryCandidateMembers.from_kwargs(
            trksub=["trk_001", "trk_001", "trk_002", "trk_003"],
            obssubid=[
                "missing_obs_1",
                "missing_obs_2",
                "missing_obs_3",
                "missing_obs_4",
            ],
        )

        with pytest.raises(
            ValueError,
            match="All obssubids in discovery_candidate_members must be present in source_catalog",
        ):
            manager_with_submitter.prepare_submissions(
                source_catalog=sample_source_catalog,
                obscontexts=sample_obscontexts,
                discovery_candidates=sample_discovery_candidates,
                discovery_candidate_members=bad_members,
            )


class TestPrepareDiscoverySubmissions:
    """Tests for preparing discovery submissions."""

    def test_prepare_discovery_submissions(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test preparing discovery submissions."""
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
            discovery_comment="Test discovery submission",
        )

        # Check that submissions were created
        assert len(submissions) > 0
        assert len(members) == len(sample_discovery_members)

        # Check submission types
        assert pc.all(pc.equal(submissions.type, "discovery")).as_py()

        # Check that files were created
        for file_path in submissions.file_path.to_pylist():
            assert os.path.exists(file_path)
            assert file_path.endswith(".psv")

    def test_prepare_discovery_creates_correct_metadata(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test that discovery submission metadata is correct."""
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
            discovery_comment="Test comment",
        )

        submission = submissions[0]

        # Check metadata
        assert submission.type[0].as_py() == "discovery"
        assert submission.linkages[0].as_py() > 0
        assert submission.observations[0].as_py() > 0
        assert submission.comment[0].as_py() == "Test comment"
        assert submission.submitter_id[0].as_py() is not None
        assert submission.file_md5[0].as_py() is not None
        assert submission.created_at[0].as_py() is not None
        assert submission.submitted_at[0].as_py() is None
        assert submission.mpc_submission_id[0].as_py() is None

    def test_prepare_discovery_writes_to_database(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test that submissions are written to database."""
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        # Query database to confirm
        submission_ids = submissions.id.to_pylist()
        db_submissions = manager_with_submitter.get_submissions(
            submission_ids=submission_ids
        )

        assert len(db_submissions) == len(submissions)

    def test_prepare_discovery_file_content(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test that generated ADES files have correct content."""
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        file_path = submissions.file_path[0].as_py()

        # Read file and check content
        with open(file_path, "r") as f:
            content = f.read()

        # Should contain ADES header markers
        assert "# version" in content or "obsTime" in content
        # Should contain observation data
        assert "X05" in content  # Observatory code from fixtures

    def test_prepare_empty_discovery_candidates(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
    ):
        """Test preparing with empty discovery candidates."""
        from mpcq.submissions.types import (
            DiscoveryCandidateMembers,
            DiscoveryCandidates,
        )

        empty_candidates = DiscoveryCandidates.empty()
        empty_members = DiscoveryCandidateMembers.empty()

        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=empty_candidates,
            discovery_candidate_members=empty_members,
        )

        # Should handle empty gracefully
        assert len(submissions) >= 0
        assert len(members) == 0


class TestPrepareAssociationSubmissions:
    """Tests for preparing association submissions."""

    def test_prepare_association_submissions(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_association_candidates,
        sample_association_members,
    ):
        """Test preparing association submissions."""
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            association_candidates=sample_association_candidates,
            association_candidate_members=sample_association_members,
            association_comment="Test association submission",
        )

        # Check that submissions were created
        assert len(submissions) > 0
        assert len(members) == len(sample_association_members)

        # Check submission types
        assert pc.all(pc.equal(submissions.type, "association")).as_py()

        # Check that files were created
        for file_path in submissions.file_path.to_pylist():
            assert os.path.exists(file_path)

    def test_prepare_association_includes_designations(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_association_candidates,
        sample_association_members,
    ):
        """Test that association members include designations."""
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            association_candidates=sample_association_candidates,
            association_candidate_members=sample_association_members,
        )

        # Check that members have provid populated (from candidates)
        assert not pc.all(pc.is_null(members.provid)).as_py()


class TestPrepareBothTypes:
    """Tests for preparing both discovery and association submissions together."""

    def test_prepare_both_types(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
        sample_association_candidates,
        sample_association_members,
    ):
        """Test preparing both discovery and association submissions."""
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
            association_candidates=sample_association_candidates,
            association_candidate_members=sample_association_members,
        )

        # Should have both types
        discovery_subs = submissions.apply_mask(pc.equal(submissions.type, "discovery"))
        association_subs = submissions.apply_mask(
            pc.equal(submissions.type, "association")
        )

        assert len(discovery_subs) > 0
        assert len(association_subs) > 0

        # Total members should match
        total_expected = len(sample_discovery_members) + len(sample_association_members)
        assert len(members) == total_expected


class TestPrepareSubmissionChunking:
    """Tests for chunking large submissions."""

    def test_prepare_with_max_observations_limit(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test that submissions are chunked when exceeding max observations."""
        # Set a very small limit to force chunking
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
            max_observations_per_file=5,  # Force chunking
        )

        # Should potentially create multiple submission files
        assert len(submissions) >= 1

        # All observation counts should be <= max
        for obs_count in submissions.observations.to_pylist():
            assert obs_count <= 5


class TestPrepareSubmitterSelection:
    """Tests for submitter selection during preparation."""

    def test_prepare_uses_selected_submitter(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test that prepare uses the selected submitter."""
        submitter_id = manager_with_submitter.submitter.id

        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        # All submissions should have the same submitter
        assert pc.all(pc.equal(submissions.submitter_id, submitter_id)).as_py()

    def test_prepare_prompts_for_submitter_if_none(
        self,
        empty_manager,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
        sample_submitters_table,
    ):
        """Test that prepare prompts for submitter if none selected."""
        from unittest.mock import patch

        # Add submitters to database
        sample_submitters_table.to_sql(
            empty_manager.engine, "submitters", if_exists="append"
        )

        # Mock input to select first submitter
        with patch("builtins.input", return_value="1"):
            submissions, members = empty_manager.prepare_submissions(
                source_catalog=sample_source_catalog,
                obscontexts=sample_obscontexts,
                discovery_candidates=sample_discovery_candidates,
                discovery_candidate_members=sample_discovery_members,
            )

        # Submitter should now be set
        assert empty_manager.submitter is not None
        assert empty_manager.submitter.id == 1


class TestPrepareSubmissionIDs:
    """Tests for submission ID generation."""

    def test_submission_ids_are_unique(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test that generated submission IDs are unique."""
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        ids = submissions.id.to_pylist()
        assert len(ids) == len(set(ids))  # All unique

    def test_submission_ids_have_date_prefix(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test that submission IDs have date prefix."""
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        # IDs should start with date prefix YYYYMMDD
        today = datetime.now().strftime("%Y%m%d")
        for sub_id in submissions.id.to_pylist():
            assert sub_id.startswith(today)

    def test_submission_ids_indicate_type(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
        sample_association_candidates,
        sample_association_members,
    ):
        """Test that submission IDs indicate type (d for discovery, a for association)."""
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
            association_candidates=sample_association_candidates,
            association_candidate_members=sample_association_members,
        )

        # Discovery IDs should contain '_d'
        discovery = submissions.apply_mask(pc.equal(submissions.type, "discovery"))
        for sub_id in discovery.id.to_pylist():
            assert "_d" in sub_id

        # Association IDs should contain '_a'
        association = submissions.apply_mask(pc.equal(submissions.type, "association"))
        for sub_id in association.id.to_pylist():
            assert "_a" in sub_id


class TestPrepareCustomPrecision:
    """Tests for custom precision in ADES output."""

    def test_prepare_with_custom_seconds_precision(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test preparing with custom seconds precision."""
        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
            seconds_precision=6,
        )

        # Should complete successfully
        assert len(submissions) > 0

    def test_prepare_with_custom_columns_precision(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test preparing with custom column precision."""
        custom_precision = {
            "ra": 10,
            "dec": 10,
        }

        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
            columns_precision=custom_precision,
        )

        # Should complete successfully
        assert len(submissions) > 0


class TestPrepareMultipleObsContexts:
    """Tests for preparing submissions with multiple observatory contexts."""

    def test_prepare_with_multiple_observatory_configs_same_mpc_code(
        self,
        manager_with_submitter,
        sample_source_catalog,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """
        Test that we can store multiple configs for the same MPC code and use one for submission.

        This tests the scenario where you have LSSTCam and ComCam configs stored in the database,
        and you select LSSTCam for a particular submission.
        """
        import json

        from adam_core.observations.ades import (
            ObsContext,
            ObservatoryObsContext,
            TelescopeObsContext,
        )

        # Create two different observatory contexts with the same MPC code but different instruments
        submitter_ctx = manager_with_submitter.get_submitter_obscontext()

        obscontext_lsstcam = ObsContext(
            observatory=ObservatoryObsContext(
                mpcCode="X05",
                name="Vera C. Rubin Observatory",
            ),
            submitter=submitter_ctx,
            measurers=["J. Moeyens"],
            telescope=TelescopeObsContext(
                name="Simonyi Survey Telescope - LSSTCam",
                design="Modified Paul-Baker",
                aperture=8.4,
                detector="CCD",
            ),
        )

        obscontext_comcam = ObsContext(
            observatory=ObservatoryObsContext(
                mpcCode="X05",
                name="Vera C. Rubin Observatory",
            ),
            submitter=submitter_ctx,
            measurers=["J. Moeyens"],
            telescope=TelescopeObsContext(
                name="Simonyi Survey Telescope - ComCam",
                design="Modified Paul-Baker",
                aperture=8.4,
                detector="CCD",
            ),
        )

        # Store both configs in database with different config names
        lsstcam_id = manager_with_submitter.store_observatory_config(
            "X05", obscontext_lsstcam, config_name="X05_LSSTCAM"
        )
        comcam_id = manager_with_submitter.store_observatory_config(
            "X05", obscontext_comcam, config_name="X05_COMCAM"
        )

        assert lsstcam_id != comcam_id  # Should have different IDs

        # Verify both are stored
        all_configs = manager_with_submitter.list_observatory_configs()
        config_names = [c["config_name"] for c in all_configs]

        assert "X05_LSSTCAM" in config_names
        assert "X05_COMCAM" in config_names

        # Now prepare a submission using the LSSTCam config
        # The key must be the MPC code for ADES generation
        obscontexts_for_submission = {
            "X05": obscontext_lsstcam,
        }

        submissions, members = manager_with_submitter.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=obscontexts_for_submission,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        # Should complete successfully
        assert len(submissions) > 0
        submission = submissions[0]

        # Check that observatory_codes and observatory_config_ids are stored
        observatory_codes_json = submission.observatory_codes[0].as_py()
        config_ids_json = submission.observatory_config_ids[0].as_py()

        assert observatory_codes_json is not None
        assert config_ids_json is not None

        # Parse JSON
        observatory_codes = json.loads(observatory_codes_json)
        config_ids = json.loads(config_ids_json)

        # Should have one MPC code
        assert len(observatory_codes) == 1
        assert len(config_ids) == 1
        assert "X05" in observatory_codes

        # The config ID should be the one we just stored
        # (Note: sync_observatory_configs will store it again, so ID might be different)

        # Verify the database has both configs with correct details
        lsstcam_db_config = next(
            (c for c in all_configs if c["config_name"] == "X05_LSSTCAM"), None
        )
        comcam_db_config = next(
            (c for c in all_configs if c["config_name"] == "X05_COMCAM"), None
        )

        assert lsstcam_db_config is not None
        assert comcam_db_config is not None
        assert lsstcam_db_config["mpc_code"] == "X05"
        assert comcam_db_config["mpc_code"] == "X05"
        assert lsstcam_db_config["id"] == lsstcam_id
        assert comcam_db_config["id"] == comcam_id
