"""
Integration tests for SubmissionManager end-to-end workflows.
"""

import os
from datetime import datetime, timedelta, timezone

import pyarrow.compute as pc
import pytest


class TestFullDiscoveryWorkflow:
    """Test complete discovery submission workflow."""

    def test_full_discovery_workflow(
        self,
        empty_manager,
        sample_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
        mock_mpc_submission_client,
    ):
        """Test complete workflow: add submitter, prepare, queue, submit."""
        # Step 1: Add submitter
        empty_manager.add_submitter(sample_submitter)
        empty_manager._submitter = sample_submitter

        # Step 2: Prepare submissions
        submissions, members = empty_manager.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
            discovery_comment="Integration test discovery",
        )

        assert len(submissions) > 0
        assert len(members) > 0

        # Step 3: Queue for submission
        submission_ids = submissions.id.to_pylist()
        empty_manager.queue_for_submission(submission_ids)

        assert empty_manager.queue.qsize() == len(submission_ids)

        # Step 4: Submit
        empty_manager.mpc_submission_client = mock_mpc_submission_client
        empty_manager.submit_queue(delay=timedelta(seconds=0))

        # Step 5: Verify all submissions were processed
        updated_submissions = empty_manager.get_submissions(
            submission_ids=submission_ids
        )

        # All should have MPC IDs and submitted timestamps
        assert not pc.any(pc.is_null(updated_submissions.mpc_submission_id)).as_py()
        assert not pc.any(pc.is_null(updated_submissions.submitted_at)).as_py()


class TestFullAssociationWorkflow:
    """Test complete association submission workflow."""

    def test_full_association_workflow(
        self,
        empty_manager,
        sample_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_association_candidates,
        sample_association_members,
        mock_mpc_submission_client,
    ):
        """Test complete association workflow."""
        # Add submitter
        empty_manager.add_submitter(sample_submitter)
        empty_manager._submitter = sample_submitter

        # Prepare submissions
        submissions, members = empty_manager.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            association_candidates=sample_association_candidates,
            association_candidate_members=sample_association_members,
            association_comment="Integration test association",
        )

        assert len(submissions) > 0

        # Queue and submit
        submission_ids = submissions.id.to_pylist()
        empty_manager.queue_for_submission(submission_ids)
        empty_manager.mpc_submission_client = mock_mpc_submission_client
        empty_manager.submit_queue(delay=timedelta(seconds=0))

        # Verify
        updated_submissions = empty_manager.get_submissions(
            submission_ids=submission_ids
        )
        assert not pc.any(pc.is_null(updated_submissions.mpc_submission_id)).as_py()


class TestMixedSubmissionWorkflow:
    """Test workflow with both discovery and association submissions."""

    def test_mixed_submission_workflow(
        self,
        empty_manager,
        sample_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
        sample_association_candidates,
        sample_association_members,
        mock_mpc_submission_client,
    ):
        """Test workflow with both discovery and association submissions."""
        # Setup
        empty_manager.add_submitter(sample_submitter)
        empty_manager._submitter = sample_submitter

        # Prepare both types
        submissions, members = empty_manager.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
            association_candidates=sample_association_candidates,
            association_candidate_members=sample_association_members,
            discovery_comment="Integration test discovery",
            association_comment="Integration test association",
        )

        # Verify both types were created
        discovery_subs = submissions.apply_mask(pc.equal(submissions.type, "discovery"))
        association_subs = submissions.apply_mask(
            pc.equal(submissions.type, "association")
        )

        assert len(discovery_subs) > 0
        assert len(association_subs) > 0

        # Submit all
        submission_ids = submissions.id.to_pylist()
        empty_manager.queue_for_submission(submission_ids)
        empty_manager.mpc_submission_client = mock_mpc_submission_client
        empty_manager.submit_queue(delay=timedelta(seconds=0))

        # Verify all were submitted
        updated_submissions = empty_manager.get_submissions(
            submission_ids=submission_ids
        )
        assert len(updated_submissions) == len(submissions)
        assert not pc.any(pc.is_null(updated_submissions.submitted_at)).as_py()


class TestLoadAndResubmitWorkflow:
    """Test loading manager from disk and resubmitting."""

    def test_load_existing_manager_and_query(
        self,
        temp_submission_dir,
        sample_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
        mock_mpc_submission_client,
    ):
        """Test creating manager, closing, reopening, and continuing work."""
        from mpcq.submissions.manager import SubmissionManager

        # Create initial manager and do some work
        manager1 = SubmissionManager.create(temp_submission_dir)
        manager1.add_submitter(sample_submitter)
        manager1._submitter = sample_submitter

        submissions, members = manager1.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        submission_ids = submissions.id.to_pylist()

        # "Close" manager by deleting it
        del manager1

        # Load from directory
        manager2 = SubmissionManager.from_dir(temp_submission_dir)

        # Verify data persisted
        loaded_submissions = manager2.get_submissions(submission_ids=submission_ids)
        assert len(loaded_submissions) == len(submissions)

        # Continue workflow with loaded manager
        manager2._submitter = sample_submitter
        manager2.queue_for_submission(submission_ids)
        manager2.mpc_submission_client = mock_mpc_submission_client
        manager2.submit_queue(delay=timedelta(seconds=0))

        # Verify submissions completed
        final_submissions = manager2.get_submissions(submission_ids=submission_ids)
        assert not pc.any(pc.is_null(final_submissions.submitted_at)).as_py()


class TestLoadQueueWorkflow:
    """Test the load_queue workflow."""

    def test_load_queue_and_submit(
        self,
        empty_manager,
        sample_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
        mock_mpc_submission_client,
    ):
        """Test preparing submissions, then using load_queue to requeue unsubmitted."""
        # Setup and prepare
        empty_manager.add_submitter(sample_submitter)
        empty_manager._submitter = sample_submitter

        submissions, members = empty_manager.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        # Don't queue or submit yet - submissions are prepared but unsubmitted

        # Load queue (should find the unsubmitted submissions)
        empty_manager.load_queue()

        # Queue should have the submissions
        assert empty_manager.queue.qsize() > 0

        # Submit them
        empty_manager.mpc_submission_client = mock_mpc_submission_client
        empty_manager.submit_queue(delay=timedelta(seconds=0))

        # Verify
        submission_ids = submissions.id.to_pylist()
        final_submissions = empty_manager.get_submissions(submission_ids=submission_ids)
        assert not pc.any(pc.is_null(final_submissions.submitted_at)).as_py()


class TestPartialFailureWorkflow:
    """Test handling partial failures in submission."""

    def test_partial_submission_failure(
        self,
        empty_manager,
        sample_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test that some submissions can fail while others succeed."""
        from unittest.mock import Mock

        # Setup
        empty_manager.add_submitter(sample_submitter)
        empty_manager._submitter = sample_submitter

        # Prepare multiple submissions
        submissions, members = empty_manager.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
            max_observations_per_file=3,  # Force multiple files
        )

        # Create mock that fails on first call, succeeds on subsequent
        mock_client = Mock()
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise ValueError("First submission failed")
            return ("mpc_id_" + str(call_count[0]), datetime.now(timezone.utc))

        mock_client.submit_ades.side_effect = side_effect

        # Queue and submit
        submission_ids = submissions.id.to_pylist()
        empty_manager.queue_for_submission(submission_ids)
        empty_manager.mpc_submission_client = mock_client

        # Submit queue - first should fail, rest should succeed
        empty_manager.submit_queue(delay=timedelta(seconds=0))

        # Check results
        updated_submissions = empty_manager.get_submissions(
            submission_ids=submission_ids
        )

        # At least one should have an error
        errors = updated_submissions.error.to_pylist()
        assert any(e is not None for e in errors)

        # At least one should have succeeded (if multiple were created)
        if len(updated_submissions) > 1:
            submitted_times = updated_submissions.submitted_at.to_pylist()
            assert any(t is not None for t in submitted_times)


class TestQueryStatusWorkflow:
    """Test querying submission status."""

    def test_query_submission_status(
        self,
        empty_manager,
        sample_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
        mock_mpc_submission_client,
        mock_mpc_sbn_client,
    ):
        """Test the full workflow including status queries."""
        # Setup and submit
        empty_manager.add_submitter(sample_submitter)
        empty_manager._submitter = sample_submitter

        submissions, members = empty_manager.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        submission_ids = submissions.id.to_pylist()
        empty_manager.queue_for_submission(submission_ids)
        empty_manager.mpc_submission_client = mock_mpc_submission_client
        empty_manager.mpc_sbn_client = mock_mpc_sbn_client
        empty_manager.submit_queue(delay=timedelta(seconds=0))

        # Query status
        updated_members = empty_manager.query_submission_status(submission_ids)

        # Should have status information
        assert len(updated_members) > 0
        # WAMO results should be populated
        assert not pc.all(pc.is_null(updated_members.mpc_status)).as_py()


class TestDeletePreparedSubmissions:
    """Test deleting prepared but unsubmitted submissions."""

    def test_delete_prepared_submissions(
        self,
        empty_manager,
        sample_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test deleting unsubmitted submissions."""
        # Setup and prepare
        empty_manager.add_submitter(sample_submitter)
        empty_manager._submitter = sample_submitter

        submissions, members = empty_manager.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        submission_ids = submissions.id.to_pylist()
        file_paths = submissions.file_path.to_pylist()

        # Verify files exist
        for fp in file_paths:
            assert os.path.exists(fp)

        # Delete prepared submissions
        empty_manager.delete_prepared_submissions(submission_ids=submission_ids)

        # Verify files were deleted
        for fp in file_paths:
            assert not os.path.exists(fp)

        # Verify database entries were deleted
        remaining = empty_manager.get_submissions(submission_ids=submission_ids)
        assert len(remaining) == 0


class TestManagerStateConsistency:
    """Test that manager maintains consistent state throughout operations."""

    def test_database_and_files_stay_consistent(
        self,
        empty_manager,
        sample_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test that database and filesystem stay in sync."""
        # Setup
        empty_manager.add_submitter(sample_submitter)
        empty_manager._submitter = sample_submitter

        # Prepare
        submissions, members = empty_manager.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        # Check consistency: every file in DB exists
        for submission in submissions:
            file_path = submission.file_path[0].as_py()
            assert os.path.exists(file_path), f"File {file_path} not found"

        # Check consistency: all members have valid submission IDs
        all_members = empty_manager.get_submission_members()
        for member in all_members:
            sub_id = member.submission_id[0].as_py()
            sub = empty_manager.get_submissions(submission_ids=[sub_id])
            assert len(sub) == 1, f"Submission {sub_id} not found for member"

    def test_queue_and_database_stay_consistent(
        self,
        empty_manager,
        sample_submitter,
        sample_source_catalog,
        sample_obscontexts,
        sample_discovery_candidates,
        sample_discovery_members,
    ):
        """Test that queue and database remain consistent."""
        # Setup
        empty_manager.add_submitter(sample_submitter)
        empty_manager._submitter = sample_submitter

        # Prepare
        submissions, members = empty_manager.prepare_submissions(
            source_catalog=sample_source_catalog,
            obscontexts=sample_obscontexts,
            discovery_candidates=sample_discovery_candidates,
            discovery_candidate_members=sample_discovery_members,
        )

        submission_ids = submissions.id.to_pylist()

        # Queue
        empty_manager.queue_for_submission(submission_ids)

        # Verify: every queued ID exists in database
        while not empty_manager.queue.empty():
            queued_id, queued_file = empty_manager.queue.get()
            sub = empty_manager.get_submissions(submission_ids=[queued_id])
            assert len(sub) == 1, f"Queued submission {queued_id} not in database"
            assert os.path.exists(queued_file), f"Queued file {queued_file} not found"
