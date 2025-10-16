"""
Tests for SubmissionManager submission execution.
"""

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pyarrow.compute as pc
import pytest

from mpcq.submissions.types import Submissions


class TestSubmitFromQueue:
    """Tests for submitting from the queue."""

    def test_submit_from_queue_requires_submitter(self, empty_manager, temp_dir):
        """Test that submit requires a submitter to be set."""
        # Create a submission
        file_path = os.path.join(temp_dir, "test.psv")
        with open(file_path, "w") as f:
            f.write("test content")

        empty_manager.queue.put(("test_id", file_path))

        with pytest.raises(ValueError, match="Submitter not set"):
            empty_manager.submit_from_queue()

    def test_submit_from_empty_queue(self, manager_with_submitter):
        """Test submitting from an empty queue."""
        # Should return without error
        manager_with_submitter.submit_from_queue()

    def test_submit_discovery_submission(
        self,
        manager_with_submitter,
        mock_mpc_submission_client,
        temp_dir,
    ):
        """Test submitting a discovery submission."""
        # Create a real submission
        submission_id = "test_sub_001"
        file_path = os.path.join(temp_dir, f"{submission_id}.psv")
        with open(file_path, "w") as f:
            f.write("# ADES PSV test content\ntest data")

        submission = Submissions.from_kwargs(
            id=[submission_id],
            mpc_submission_id=[None],
            submitter_id=[manager_with_submitter.submitter.id],
            type=["discovery"],
            linkages=[1],
            observations=[5],
            first_observation_mjd_utc=[60000.0],
            last_observation_mjd_utc=[60001.0],
            created_at=[datetime.now(timezone.utc)],
            submitted_at=[None],
            file_path=[file_path],
            file_md5=["abc123"],
            comment=["Test submission"],
            error=[None],
        )

        submission.to_sql(
            manager_with_submitter.engine, "submissions", if_exists="append"
        )

        # Add member
        from mpcq.submissions.types import SubmissionMembers

        members = SubmissionMembers.from_kwargs(
            submission_id=[submission_id] * 5,
            trksub=["trk_001"] * 5,
            obssubid=[f"obs_{i}" for i in range(5)],
        )
        members.to_sql(
            manager_with_submitter.engine, "submission_members", if_exists="append"
        )

        # Set mock client
        manager_with_submitter.mpc_submission_client = mock_mpc_submission_client

        # Queue and submit
        manager_with_submitter.queue.put((submission_id, file_path))
        manager_with_submitter.submit_from_queue()

        # Verify submission was called
        mock_mpc_submission_client.submit_ades.assert_called_once()

        # Check that submission was marked as submitted in database
        updated_submission = manager_with_submitter.get_submissions(
            submission_ids=[submission_id]
        )
        assert updated_submission.mpc_submission_id[0].as_py() is not None
        assert updated_submission.submitted_at[0].as_py() is not None

    def test_submit_association_submission(
        self,
        manager_with_submitter,
        mock_mpc_submission_client,
        temp_dir,
    ):
        """Test submitting an association submission."""
        submission_id = "test_assoc_001"
        file_path = os.path.join(temp_dir, f"{submission_id}.psv")
        with open(file_path, "w") as f:
            f.write("# ADES PSV test content\ntest data")

        submission = Submissions.from_kwargs(
            id=[submission_id],
            mpc_submission_id=[None],
            submitter_id=[manager_with_submitter.submitter.id],
            type=["association"],
            linkages=[1],
            observations=[3],
            first_observation_mjd_utc=[60000.0],
            last_observation_mjd_utc=[60001.0],
            created_at=[datetime.now(timezone.utc)],
            submitted_at=[None],
            file_path=[file_path],
            file_md5=["abc123"],
            comment=["Test association"],
            error=[None],
        )

        submission.to_sql(
            manager_with_submitter.engine, "submissions", if_exists="append"
        )

        # Add member
        from mpcq.submissions.types import SubmissionMembers

        members = SubmissionMembers.from_kwargs(
            submission_id=[submission_id] * 3,
            trksub=["trk_101"] * 3,
            provid=["2023 AA"] * 3,
            obssubid=[f"obs_{i}" for i in range(3)],
        )
        members.to_sql(
            manager_with_submitter.engine, "submission_members", if_exists="append"
        )

        # Set mock client
        manager_with_submitter.mpc_submission_client = mock_mpc_submission_client

        # Queue and submit
        manager_with_submitter.queue.put((submission_id, file_path))
        manager_with_submitter.submit_from_queue()

        # Verify submission was called
        mock_mpc_submission_client.submit_ades.assert_called_once()

    def test_submit_handles_error(
        self,
        manager_with_submitter,
        temp_dir,
    ):
        """Test that submission errors are handled gracefully."""
        submission_id = "test_error_001"
        file_path = os.path.join(temp_dir, f"{submission_id}.psv")
        with open(file_path, "w") as f:
            f.write("test content")

        submission = Submissions.from_kwargs(
            id=[submission_id],
            mpc_submission_id=[None],
            submitter_id=[manager_with_submitter.submitter.id],
            type=["discovery"],
            linkages=[1],
            observations=[1],
            first_observation_mjd_utc=[60000.0],
            last_observation_mjd_utc=[60001.0],
            created_at=[datetime.now(timezone.utc)],
            submitted_at=[None],
            file_path=[file_path],
            file_md5=["abc123"],
            comment=["Test"],
            error=[None],
        )

        submission.to_sql(
            manager_with_submitter.engine, "submissions", if_exists="append"
        )

        # Add member
        from mpcq.submissions.types import SubmissionMembers

        members = SubmissionMembers.from_kwargs(
            submission_id=[submission_id],
            trksub=["trk_001"],
            obssubid=["obs_001"],
        )
        members.to_sql(
            manager_with_submitter.engine, "submission_members", if_exists="append"
        )

        # Set mock client that raises error
        mock_client = Mock()
        mock_client.submit_ades.side_effect = ValueError("Test error")
        manager_with_submitter.mpc_submission_client = mock_client

        # Queue and try to submit
        manager_with_submitter.queue.put((submission_id, file_path))
        manager_with_submitter.submit_from_queue()

        # Check that error was recorded
        updated_submission = manager_with_submitter.get_submissions(
            submission_ids=[submission_id]
        )
        assert updated_submission.error[0].as_py() is not None
        assert "Test error" in updated_submission.error[0].as_py()
        assert updated_submission.submitted_at[0].as_py() is None

    def test_submit_missing_submission_raises_error(
        self, manager_with_submitter, mock_mpc_submission_client
    ):
        """Test that submitting a missing submission raises an error."""
        manager_with_submitter.mpc_submission_client = mock_mpc_submission_client

        # Queue a nonexistent submission
        manager_with_submitter.queue.put(("nonexistent", "/fake/path.psv"))

        with pytest.raises(ValueError, match="not found in the database"):
            manager_with_submitter.submit_from_queue()

    def test_submit_includes_md5_in_comment(
        self,
        manager_with_submitter,
        mock_mpc_submission_client,
        temp_dir,
    ):
        """Test that submission includes file MD5 in comment."""
        submission_id = "test_md5_001"
        file_path = os.path.join(temp_dir, f"{submission_id}.psv")
        with open(file_path, "w") as f:
            f.write("test content")

        submission = Submissions.from_kwargs(
            id=[submission_id],
            mpc_submission_id=[None],
            submitter_id=[manager_with_submitter.submitter.id],
            type=["discovery"],
            linkages=[1],
            observations=[1],
            first_observation_mjd_utc=[60000.0],
            last_observation_mjd_utc=[60001.0],
            created_at=[datetime.now(timezone.utc)],
            submitted_at=[None],
            file_path=[file_path],
            file_md5=["test_md5_hash"],
            comment=["Original comment"],
            error=[None],
        )

        submission.to_sql(
            manager_with_submitter.engine, "submissions", if_exists="append"
        )

        # Add member
        from mpcq.submissions.types import SubmissionMembers

        members = SubmissionMembers.from_kwargs(
            submission_id=[submission_id],
            trksub=["trk_001"],
            obssubid=["obs_001"],
        )
        members.to_sql(
            manager_with_submitter.engine, "submission_members", if_exists="append"
        )

        manager_with_submitter.mpc_submission_client = mock_mpc_submission_client

        # Queue and submit
        manager_with_submitter.queue.put((submission_id, file_path))
        manager_with_submitter.submit_from_queue()

        # Check that MD5 was included in the comment
        call_args = mock_mpc_submission_client.submit_ades.call_args
        comment_arg = call_args[0][2]  # Third positional argument
        assert "test_md5_hash" in comment_arg


class TestSubmitQueue:
    """Tests for submitting the entire queue."""

    def test_submit_queue_empty(
        self, manager_with_submitter, mock_mpc_submission_client
    ):
        """Test submitting an empty queue."""
        manager_with_submitter.mpc_submission_client = mock_mpc_submission_client
        manager_with_submitter.submit_queue()
        # Should complete without error

    def test_submit_queue_multiple_submissions(
        self,
        manager_with_submitter,
        mock_mpc_submission_client,
        temp_dir,
    ):
        """Test submitting multiple submissions from queue."""
        submission_ids = ["sub_001", "sub_002", "sub_003"]

        # Create submissions
        for idx, sub_id in enumerate(submission_ids):
            file_path = os.path.join(temp_dir, f"{sub_id}.psv")
            with open(file_path, "w") as f:
                f.write("test content")

            submission = Submissions.from_kwargs(
                id=[sub_id],
                mpc_submission_id=[None],
                submitter_id=[manager_with_submitter.submitter.id],
                type=["discovery"],
                linkages=[1],
                observations=[1],
                first_observation_mjd_utc=[60000.0],
                last_observation_mjd_utc=[60001.0],
                created_at=[datetime.now(timezone.utc)],
                submitted_at=[None],
                file_path=[file_path],
                file_md5=["abc123"],
                comment=["Test"],
                error=[None],
            )
            submission.to_sql(
                manager_with_submitter.engine, "submissions", if_exists="append"
            )

            # Add member with unique obssubid
            from mpcq.submissions.types import SubmissionMembers

            members = SubmissionMembers.from_kwargs(
                submission_id=[sub_id],
                trksub=["trk_001"],
                obssubid=[f"obs_{idx:03d}"],
            )
            members.to_sql(
                manager_with_submitter.engine, "submission_members", if_exists="append"
            )

            # Queue it
            manager_with_submitter.queue.put((sub_id, file_path))

        manager_with_submitter.mpc_submission_client = mock_mpc_submission_client

        # Submit all
        manager_with_submitter.submit_queue(delay=timedelta(seconds=0))

        # All should be submitted
        assert mock_mpc_submission_client.submit_ades.call_count == 3

    def test_submit_queue_with_delay(
        self,
        manager_with_submitter,
        mock_mpc_submission_client,
        temp_dir,
    ):
        """Test that submit_queue respects the delay parameter."""
        import time

        submission_ids = ["sub_001", "sub_002"]

        for idx, sub_id in enumerate(submission_ids):
            file_path = os.path.join(temp_dir, f"{sub_id}.psv")
            with open(file_path, "w") as f:
                f.write("test content")

            submission = Submissions.from_kwargs(
                id=[sub_id],
                mpc_submission_id=[None],
                submitter_id=[manager_with_submitter.submitter.id],
                type=["discovery"],
                linkages=[1],
                observations=[1],
                first_observation_mjd_utc=[60000.0],
                last_observation_mjd_utc=[60001.0],
                created_at=[datetime.now(timezone.utc)],
                submitted_at=[None],
                file_path=[file_path],
                file_md5=["abc123"],
                comment=["Test"],
                error=[None],
            )
            submission.to_sql(
                manager_with_submitter.engine, "submissions", if_exists="append"
            )

            # Add member with unique obssubid
            from mpcq.submissions.types import SubmissionMembers

            members = SubmissionMembers.from_kwargs(
                submission_id=[sub_id],
                trksub=["trk_001"],
                obssubid=[f"obs_delay_{idx:03d}"],
            )
            members.to_sql(
                manager_with_submitter.engine, "submission_members", if_exists="append"
            )

            manager_with_submitter.queue.put((sub_id, file_path))

        manager_with_submitter.mpc_submission_client = mock_mpc_submission_client

        # Time the submission
        start = time.time()
        manager_with_submitter.submit_queue(delay=timedelta(seconds=0.1))
        elapsed = time.time() - start

        # Should take at least 0.1 seconds (1 delay between 2 submissions)
        assert elapsed >= 0.1


class TestSubmissionStatusUpdate:
    """Tests for updating submission status."""

    def test_set_submission_success(self, empty_manager, temp_dir):
        """Test marking a submission as successful."""
        submission_id = "test_success"
        file_path = os.path.join(temp_dir, f"{submission_id}.psv")
        with open(file_path, "w") as f:
            f.write("test")

        submission = Submissions.from_kwargs(
            id=[submission_id],
            mpc_submission_id=[None],
            submitter_id=[1],
            type=["discovery"],
            linkages=[1],
            observations=[1],
            first_observation_mjd_utc=[60000.0],
            last_observation_mjd_utc=[60001.0],
            created_at=[datetime.now(timezone.utc)],
            submitted_at=[None],
            file_path=[file_path],
            file_md5=["abc123"],
            comment=["Test"],
            error=[None],
        )
        submission.to_sql(empty_manager.engine, "submissions", if_exists="append")

        # Mark as successful
        mpc_id = "mpc_submission_12345"
        submitted_time = datetime.now(timezone.utc)
        empty_manager._set_submission_success(submission_id, mpc_id, submitted_time)

        # Verify update
        updated = empty_manager.get_submissions(submission_ids=[submission_id])
        assert updated.mpc_submission_id[0].as_py() == mpc_id
        assert updated.submitted_at[0].as_py() is not None
        assert updated.error[0].as_py() is None

    def test_set_submission_success_raises_if_already_submitted(
        self, empty_manager, temp_dir
    ):
        """Test that marking already-submitted submission raises error."""
        submission_id = "already_submitted"
        file_path = os.path.join(temp_dir, f"{submission_id}.psv")
        with open(file_path, "w") as f:
            f.write("test")

        submission = Submissions.from_kwargs(
            id=[submission_id],
            mpc_submission_id=["existing_mpc_id"],
            submitter_id=[1],
            type=["discovery"],
            linkages=[1],
            observations=[1],
            first_observation_mjd_utc=[60000.0],
            last_observation_mjd_utc=[60001.0],
            created_at=[datetime.now(timezone.utc)],
            submitted_at=[datetime.now(timezone.utc)],  # Already submitted
            file_path=[file_path],
            file_md5=["abc123"],
            comment=["Test"],
            error=[None],
        )
        submission.to_sql(empty_manager.engine, "submissions", if_exists="append")

        # Try to mark as successful again
        with pytest.raises(ValueError, match="already been marked as submitted"):
            empty_manager._set_submission_success(
                submission_id, "new_mpc_id", datetime.now(timezone.utc)
            )

    def test_set_submission_failure(self, empty_manager, temp_dir):
        """Test marking a submission as failed."""
        submission_id = "test_failure"
        file_path = os.path.join(temp_dir, f"{submission_id}.psv")
        with open(file_path, "w") as f:
            f.write("test")

        submission = Submissions.from_kwargs(
            id=[submission_id],
            mpc_submission_id=[None],
            submitter_id=[1],
            type=["discovery"],
            linkages=[1],
            observations=[1],
            first_observation_mjd_utc=[60000.0],
            last_observation_mjd_utc=[60001.0],
            created_at=[datetime.now(timezone.utc)],
            submitted_at=[None],
            file_path=[file_path],
            file_md5=["abc123"],
            comment=["Test"],
            error=[None],
        )
        submission.to_sql(empty_manager.engine, "submissions", if_exists="append")

        # Mark as failed
        error = ValueError("Test error")
        empty_manager._set_submission_failure(submission_id, error)

        # Verify update
        updated = empty_manager.get_submissions(submission_ids=[submission_id])
        assert updated.error[0].as_py() == str(error)
        assert updated.submitted_at[0].as_py() is None
