"""
Tests for SubmissionManager queue operations.
"""

import os
import tempfile
from datetime import datetime, timedelta, timezone

import pyarrow.compute as pc
import pytest

from mpcq.submissions.types import Submissions


class TestQueueInitialization:
    """Tests for queue initialization."""

    def test_queue_exists_on_init(self, empty_manager):
        """Test that queue exists after initialization."""
        assert empty_manager.queue is not None

    def test_queue_starts_empty(self, empty_manager):
        """Test that queue starts empty."""
        assert empty_manager.queue.qsize() == 0

    def test_queue_is_fifo(self, empty_manager):
        """Test that queue follows FIFO order."""
        # Add items to queue
        empty_manager.queue.put(("id1", "file1.psv"))
        empty_manager.queue.put(("id2", "file2.psv"))
        empty_manager.queue.put(("id3", "file3.psv"))

        # Retrieve in order
        assert empty_manager.queue.get() == ("id1", "file1.psv")
        assert empty_manager.queue.get() == ("id2", "file2.psv")
        assert empty_manager.queue.get() == ("id3", "file3.psv")


class TestClearQueue:
    """Tests for clearing the queue."""

    def test_clear_empty_queue(self, empty_manager):
        """Test clearing an already empty queue."""
        empty_manager.clear_queue()
        assert empty_manager.queue.qsize() == 0

    def test_clear_queue_with_items(self, empty_manager):
        """Test clearing a queue with items."""
        # Add items
        empty_manager.queue.put(("id1", "file1.psv"))
        empty_manager.queue.put(("id2", "file2.psv"))
        assert empty_manager.queue.qsize() == 2

        # Clear
        empty_manager.clear_queue()
        assert empty_manager.queue.qsize() == 0

    def test_queue_usable_after_clear(self, empty_manager):
        """Test that queue is usable after clearing."""
        # Add, clear, add again
        empty_manager.queue.put(("id1", "file1.psv"))
        empty_manager.clear_queue()
        empty_manager.queue.put(("id2", "file2.psv"))

        assert empty_manager.queue.qsize() == 1
        assert empty_manager.queue.get() == ("id2", "file2.psv")


class TestQueueForSubmission:
    """Tests for queuing submissions."""

    def test_queue_single_submission(self, empty_manager, temp_dir):
        """Test queuing a single submission."""
        # Create a submission with a real file
        submission_id = "test_submission_001"
        file_path = os.path.join(temp_dir, f"{submission_id}.psv")
        with open(file_path, "w") as f:
            f.write("test content")

        submission = Submissions.from_kwargs(
            id=[submission_id],
            mpc_submission_id=[None],
            submitter_id=[1],
            type=["discovery"],
            linkages=[1],
            observations=[5],
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

        # Queue it
        empty_manager.queue_for_submission([submission_id])

        # Check queue
        assert empty_manager.queue.qsize() == 1
        queued_id, queued_file = empty_manager.queue.get()
        assert queued_id == submission_id
        assert queued_file == file_path

    def test_queue_multiple_submissions(self, empty_manager, temp_dir):
        """Test queuing multiple submissions."""
        submission_ids = ["sub_001", "sub_002", "sub_003"]
        file_paths = []

        # Create submissions with real files
        for sub_id in submission_ids:
            file_path = os.path.join(temp_dir, f"{sub_id}.psv")
            with open(file_path, "w") as f:
                f.write("test content")
            file_paths.append(file_path)

        submissions = Submissions.from_kwargs(
            id=submission_ids,
            mpc_submission_id=[None] * 3,
            submitter_id=[1] * 3,
            type=["discovery"] * 3,
            linkages=[1] * 3,
            observations=[5] * 3,
            first_observation_mjd_utc=[60000.0] * 3,
            last_observation_mjd_utc=[60001.0] * 3,
            created_at=[datetime.now(timezone.utc)] * 3,
            submitted_at=[None] * 3,
            file_path=file_paths,
            file_md5=["abc123"] * 3,
            comment=["Test"] * 3,
            error=[None] * 3,
        )

        submissions.to_sql(empty_manager.engine, "submissions", if_exists="append")

        # Queue all
        empty_manager.queue_for_submission(submission_ids)

        # Check queue size
        assert empty_manager.queue.qsize() == 3

    def test_queue_nonexistent_submission_raises_error(self, empty_manager):
        """Test that queuing a nonexistent submission raises an error."""
        with pytest.raises(ValueError, match="No submissions found"):
            empty_manager.queue_for_submission(["nonexistent"])

    def test_queue_already_submitted_raises_error(self, empty_manager, temp_dir):
        """Test that queuing an already submitted submission raises an error."""
        submission_id = "already_submitted"
        file_path = os.path.join(temp_dir, f"{submission_id}.psv")
        with open(file_path, "w") as f:
            f.write("test content")

        # Create submission that's already been submitted
        submission = Submissions.from_kwargs(
            id=[submission_id],
            mpc_submission_id=["mpc_id_123"],
            submitter_id=[1],
            type=["discovery"],
            linkages=[1],
            observations=[5],
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

        # Try to queue it
        with pytest.raises(ValueError, match="already been submitted"):
            empty_manager.queue_for_submission([submission_id])

    def test_queue_submission_with_missing_file_raises_error(self, empty_manager):
        """Test that queuing a submission with missing file raises an error."""
        submission_id = "missing_file"
        file_path = "/nonexistent/path/file.psv"

        submission = Submissions.from_kwargs(
            id=[submission_id],
            mpc_submission_id=[None],
            submitter_id=[1],
            type=["discovery"],
            linkages=[1],
            observations=[5],
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

        # Try to queue it - should raise assertion error
        with pytest.raises(AssertionError, match="does not exist"):
            empty_manager.queue_for_submission([submission_id])


class TestLoadQueue:
    """Tests for loading the queue."""

    def test_load_queue_empty_database(self, empty_manager):
        """Test loading queue from empty database."""
        empty_manager.load_queue()
        assert empty_manager.queue.qsize() == 0

    def test_load_queue_with_unsubmitted_submissions(self, empty_manager, temp_dir):
        """Test loading queue with unsubmitted submissions."""
        # Create recent unsubmitted submissions
        submission_ids = ["sub_001", "sub_002"]
        file_paths = []

        for sub_id in submission_ids:
            file_path = os.path.join(temp_dir, f"{sub_id}.psv")
            with open(file_path, "w") as f:
                f.write("test content")
            file_paths.append(file_path)

        submissions = Submissions.from_kwargs(
            id=submission_ids,
            mpc_submission_id=[None] * 2,
            submitter_id=[1] * 2,
            type=["discovery"] * 2,
            linkages=[1] * 2,
            observations=[5] * 2,
            first_observation_mjd_utc=[60000.0] * 2,
            last_observation_mjd_utc=[60001.0] * 2,
            created_at=[datetime.now(timezone.utc)] * 2,
            submitted_at=[None] * 2,
            file_path=file_paths,
            file_md5=["abc123"] * 2,
            comment=["Test"] * 2,
            error=[None] * 2,
        )

        submissions.to_sql(empty_manager.engine, "submissions", if_exists="append")

        # Load queue
        empty_manager.load_queue()

        # Should have both submissions
        assert empty_manager.queue.qsize() == 2

    def test_load_queue_ignores_submitted_submissions(self, empty_manager, temp_dir):
        """Test that load_queue ignores already submitted submissions."""
        # Create one submitted and one unsubmitted
        file_path1 = os.path.join(temp_dir, "sub_001.psv")
        file_path2 = os.path.join(temp_dir, "sub_002.psv")
        for fp in [file_path1, file_path2]:
            with open(fp, "w") as f:
                f.write("test content")

        submissions = Submissions.from_kwargs(
            id=["sub_001", "sub_002"],
            mpc_submission_id=["mpc_123", None],  # First is submitted
            submitter_id=[1, 1],
            type=["discovery", "discovery"],
            linkages=[1, 1],
            observations=[5, 5],
            first_observation_mjd_utc=[60000.0, 60000.0],
            last_observation_mjd_utc=[60001.0, 60001.0],
            created_at=[datetime.now(timezone.utc), datetime.now(timezone.utc)],
            submitted_at=[datetime.now(timezone.utc), None],  # First is submitted
            file_path=[file_path1, file_path2],
            file_md5=["abc123", "def456"],
            comment=["Test", "Test"],
            error=[None, None],
        )

        submissions.to_sql(empty_manager.engine, "submissions", if_exists="append")

        # Load queue
        empty_manager.load_queue()

        # Should only have the unsubmitted one
        assert empty_manager.queue.qsize() == 1

    def test_load_queue_ignores_old_submissions(self, empty_manager, temp_dir):
        """Test that load_queue ignores submissions older than 1 day."""
        file_path = os.path.join(temp_dir, "old_sub.psv")
        with open(file_path, "w") as f:
            f.write("test content")

        # Create old submission (2 days ago)
        old_date = datetime.now(timezone.utc) - timedelta(days=2)
        submission = Submissions.from_kwargs(
            id=["old_sub"],
            mpc_submission_id=[None],
            submitter_id=[1],
            type=["discovery"],
            linkages=[1],
            observations=[5],
            first_observation_mjd_utc=[60000.0],
            last_observation_mjd_utc=[60001.0],
            created_at=[old_date],
            submitted_at=[None],
            file_path=[file_path],
            file_md5=["abc123"],
            comment=["Test"],
            error=[None],
        )

        submission.to_sql(empty_manager.engine, "submissions", if_exists="append")

        # Load queue
        empty_manager.load_queue()

        # Should not include old submission
        assert empty_manager.queue.qsize() == 0

    def test_load_queue_clears_existing_queue(self, empty_manager, temp_dir):
        """Test that load_queue clears the existing queue first."""
        # Put something in the queue manually
        empty_manager.queue.put(("manual_id", "manual_file.psv"))
        assert empty_manager.queue.qsize() == 1

        # Load queue (no submissions in database)
        empty_manager.load_queue()

        # Queue should be cleared
        assert empty_manager.queue.qsize() == 0


class TestQueueStateManagement:
    """Tests for queue state management."""

    def test_queue_size_after_operations(self, empty_manager):
        """Test queue size changes correctly with operations."""
        assert empty_manager.queue.qsize() == 0

        # Add items
        empty_manager.queue.put(("id1", "file1.psv"))
        assert empty_manager.queue.qsize() == 1

        empty_manager.queue.put(("id2", "file2.psv"))
        assert empty_manager.queue.qsize() == 2

        # Remove item
        empty_manager.queue.get()
        assert empty_manager.queue.qsize() == 1

        # Remove last item
        empty_manager.queue.get()
        assert empty_manager.queue.qsize() == 0

    def test_queue_empty_check(self, empty_manager):
        """Test checking if queue is empty."""
        assert empty_manager.queue.empty()

        empty_manager.queue.put(("id1", "file1.psv"))
        assert not empty_manager.queue.empty()

        empty_manager.queue.get()
        assert empty_manager.queue.empty()
