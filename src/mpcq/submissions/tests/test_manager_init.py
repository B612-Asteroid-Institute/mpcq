"""
Tests for SubmissionManager initialization and setup.
"""

import os

import pytest
import sqlalchemy as sq

from mpcq.submissions.manager import SubmissionManager


class TestManagerCreation:
    """Tests for creating a new SubmissionManager."""

    def test_create_new_manager(self, temp_submission_dir):
        """Test creating a new SubmissionManager."""
        manager = SubmissionManager.create(temp_submission_dir)

        # Check that the manager was created
        assert manager is not None
        assert isinstance(manager, SubmissionManager)

        # Check that the database was created
        db_path = os.path.join(temp_submission_dir, "tracking.db")
        assert os.path.exists(db_path)

        # Check that the submission directory was created
        submissions_dir = os.path.join(temp_submission_dir, "submissions")
        assert os.path.exists(submissions_dir)
        assert os.path.isdir(submissions_dir)

    def test_create_manager_with_existing_database(self, temp_submission_dir):
        """Test that creating a manager with an existing database raises an error."""
        # Create the first manager
        SubmissionManager.create(temp_submission_dir)

        # Try to create another manager in the same directory
        with pytest.raises(FileExistsError, match="already exists"):
            SubmissionManager.create(temp_submission_dir)

    def test_create_manager_creates_tables(self, temp_submission_dir):
        """Test that creating a manager creates the required tables."""
        manager = SubmissionManager.create(temp_submission_dir)

        # Check that the required tables exist
        expected_tables = ["submissions", "submission_members", "submitters"]
        for table_name in expected_tables:
            assert table_name in manager.tables

        # Verify table structure for submissions
        submissions_table = manager.tables["submissions"]
        expected_columns = [
            "id",
            "mpc_submission_id",
            "submitter_id",
            "type",
            "linkages",
            "observations",
            "first_observation_mjd_utc",
            "last_observation_mjd_utc",
            "created_at",
            "submitted_at",
            "file_path",
            "file_md5",
            "comment",
            "error",
        ]
        for col_name in expected_columns:
            assert col_name in submissions_table.columns

        # Verify table structure for submission_members
        members_table = manager.tables["submission_members"]
        expected_columns = [
            "submission_id",
            "permid",
            "provid",
            "trksub",
            "obssubid",
            "mpc_obsid",
            "mpc_status",
            "mpc_permid",
            "mpc_provid",
            "updated_at",
        ]
        for col_name in expected_columns:
            assert col_name in members_table.columns

        # Verify table structure for submitters
        submitters_table = manager.tables["submitters"]
        expected_columns = [
            "id",
            "first_name",
            "last_name",
            "email",
            "institution",
            "created_at",
        ]
        for col_name in expected_columns:
            assert col_name in submitters_table.columns

    def test_manager_directory_structure(self, temp_submission_dir):
        """Test that the manager creates the correct directory structure."""
        manager = SubmissionManager.create(temp_submission_dir)

        assert manager.directory == temp_submission_dir
        assert manager.submission_directory == os.path.join(
            temp_submission_dir, "submissions"
        )

        # Verify paths exist
        assert os.path.exists(manager.directory)
        assert os.path.exists(manager.submission_directory)


class TestManagerLoading:
    """Tests for loading an existing SubmissionManager."""

    def test_load_from_existing_directory(self, temp_submission_dir):
        """Test loading a manager from an existing directory."""
        # Create a manager
        original_manager = SubmissionManager.create(temp_submission_dir)

        # Load it from the directory
        loaded_manager = SubmissionManager.from_dir(temp_submission_dir)

        # Check that the loaded manager has the same properties
        assert loaded_manager.directory == original_manager.directory
        assert (
            loaded_manager.submission_directory == original_manager.submission_directory
        )

        # Check that tables exist
        assert len(loaded_manager.tables) == len(original_manager.tables)
        for table_name in original_manager.tables:
            assert table_name in loaded_manager.tables

    def test_load_from_nonexistent_directory(self, temp_dir):
        """Test that loading from a nonexistent directory raises an error."""
        nonexistent_dir = os.path.join(temp_dir, "nonexistent")

        with pytest.raises(FileNotFoundError, match="No database found"):
            SubmissionManager.from_dir(nonexistent_dir)

    def test_load_from_directory_without_database(self, temp_dir):
        """Test that loading from a directory without a database raises an error."""
        empty_dir = os.path.join(temp_dir, "empty")
        os.makedirs(empty_dir)

        with pytest.raises(FileNotFoundError, match="No database found"):
            SubmissionManager.from_dir(empty_dir)


class TestManagerLogging:
    """Tests for SubmissionManager logging setup."""

    def test_logging_setup(self, empty_manager):
        """Test that logging is set up correctly."""
        assert empty_manager.logger is not None
        assert empty_manager.logger.name == "SubmissionManager"

        # Check that the log file was created
        log_file = os.path.join(empty_manager.directory, "manager.log")
        assert os.path.exists(log_file)

    def test_log_file_creation(self, temp_submission_dir):
        """Test that the log file is created during initialization."""
        manager = SubmissionManager.create(temp_submission_dir)

        log_file = os.path.join(temp_submission_dir, "manager.log")
        assert os.path.exists(log_file)

        # Check that some content was written
        with open(log_file, "r") as f:
            content = f.read()
            assert "SubmissionManager initialized" in content


class TestManagerProperties:
    """Tests for SubmissionManager properties."""

    def test_queue_property_is_initialized(self, empty_manager):
        """Test that the queue property is initialized."""
        assert empty_manager.queue is not None
        assert empty_manager.queue.qsize() == 0

    def test_queue_property_is_readonly(self, empty_manager):
        """Test that the queue property cannot be set."""
        import queue as qu

        with pytest.raises(NotImplementedError, match="read-only"):
            empty_manager.queue = qu.Queue()

    def test_queue_property_cannot_be_deleted(self, empty_manager):
        """Test that the queue property cannot be deleted."""
        with pytest.raises(NotImplementedError, match="read-only"):
            del empty_manager.queue

    def test_submitter_property_starts_none(self, empty_manager):
        """Test that the submitter property starts as None."""
        assert empty_manager.submitter is None

    def test_submitter_property_can_be_deleted(self, manager_with_submitter):
        """Test that the submitter property can be deleted."""
        assert manager_with_submitter.submitter is not None

        del manager_with_submitter.submitter

        assert manager_with_submitter.submitter is None

    def test_mpc_submission_client_property(
        self, empty_manager, mock_mpc_submission_client
    ):
        """Test the mpc_submission_client property."""
        assert empty_manager.mpc_submission_client is None

        # Set the client
        empty_manager.mpc_submission_client = mock_mpc_submission_client
        assert empty_manager.mpc_submission_client == mock_mpc_submission_client

        # Delete the client
        del empty_manager.mpc_submission_client
        assert empty_manager.mpc_submission_client is None

    def test_mpc_sbn_client_property(self, empty_manager, mock_mpc_sbn_client):
        """Test the mpc_sbn_client property."""
        # Set the client
        empty_manager.mpc_sbn_client = mock_mpc_sbn_client
        assert empty_manager.mpc_sbn_client == mock_mpc_sbn_client

        # Delete the client
        del empty_manager.mpc_sbn_client
        assert empty_manager.mpc_sbn_client is None


class TestDatabaseEngine:
    """Tests for database engine and connection."""

    def test_engine_is_sqlite(self, empty_manager):
        """Test that the engine is SQLite."""
        assert isinstance(empty_manager.engine, sq.engine.base.Engine)
        assert "sqlite" in str(empty_manager.engine.url)

    def test_database_connection_works(self, empty_manager):
        """Test that we can connect to the database."""
        with empty_manager.engine.begin() as conn:
            # Try a simple query
            result = conn.execute(sq.text("SELECT 1"))
            assert result.fetchone()[0] == 1

    def test_tables_can_be_queried(self, empty_manager):
        """Test that we can query the tables."""
        with empty_manager.engine.begin() as conn:
            # Query each table
            for table_name in ["submissions", "submission_members", "submitters"]:
                result = conn.execute(sq.select(empty_manager.tables[table_name]))
                # Should return empty result
                assert len(result.fetchall()) == 0
