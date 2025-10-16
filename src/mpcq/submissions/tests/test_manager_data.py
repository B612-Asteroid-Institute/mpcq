"""
Tests for SubmissionManager data retrieval operations.
"""

from datetime import datetime, timedelta, timezone

import pyarrow.compute as pc
import pytest


class TestGetSubmissions:
    """Tests for retrieving submissions from the database."""

    def test_get_all_submissions(self, empty_manager, sample_submissions):
        """Test getting all submissions."""
        # Add submissions to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )

        submissions = empty_manager.get_submissions()
        assert len(submissions) == len(sample_submissions)

    def test_get_submissions_by_id(self, empty_manager, sample_submissions):
        """Test getting specific submissions by ID."""
        # Add submissions to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )

        # Get submission with specific ID
        submission_id = sample_submissions.id[0].as_py()
        submissions = empty_manager.get_submissions(submission_ids=[submission_id])

        assert len(submissions) == 1
        assert submissions.id[0].as_py() == submission_id

    def test_get_submissions_by_multiple_ids(self, empty_manager, sample_submissions):
        """Test getting multiple submissions by ID."""
        # Add submissions to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )

        # Get both submissions
        all_ids = sample_submissions.id.to_pylist()
        submissions = empty_manager.get_submissions(submission_ids=all_ids)

        assert len(submissions) == len(sample_submissions)

    def test_get_submissions_since_date(self, empty_manager, sample_submissions):
        """Test getting submissions since a specific date."""
        # Add submissions to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )

        # Get submissions since yesterday
        since = datetime.now(timezone.utc) - timedelta(days=1)
        submissions = empty_manager.get_submissions(since=since)

        # All submissions should be returned (created today)
        assert len(submissions) == len(sample_submissions)

    def test_get_submissions_until_date(self, empty_manager, sample_submissions):
        """Test getting submissions until a specific date."""
        # Add submissions to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )

        # Get submissions until tomorrow
        until = datetime.now(timezone.utc) + timedelta(days=1)
        submissions = empty_manager.get_submissions(until=until)

        # All submissions should be returned
        assert len(submissions) == len(sample_submissions)

    def test_get_submissions_date_range(self, empty_manager, sample_submissions):
        """Test getting submissions within a date range."""
        # Add submissions to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )

        # Get submissions in a date range
        since = datetime.now(timezone.utc) - timedelta(days=1)
        until = datetime.now(timezone.utc) + timedelta(days=1)
        submissions = empty_manager.get_submissions(since=since, until=until)

        assert len(submissions) == len(sample_submissions)

    def test_get_submissions_empty_database(self, empty_manager):
        """Test getting submissions from an empty database."""
        submissions = empty_manager.get_submissions()
        assert len(submissions) == 0

    def test_get_nonexistent_submission(self, empty_manager):
        """Test getting a submission that doesn't exist."""
        submissions = empty_manager.get_submissions(submission_ids=["nonexistent"])
        assert len(submissions) == 0

    def test_get_submissions_excludes_old_dates(
        self, empty_manager, sample_submissions
    ):
        """Test that old submissions are excluded when using since parameter."""
        # Add submissions to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )

        # Get submissions since tomorrow (should be empty)
        since = datetime.now(timezone.utc) + timedelta(days=1)
        submissions = empty_manager.get_submissions(since=since)

        assert len(submissions) == 0

    def test_get_submissions_returns_correct_columns(
        self, empty_manager, sample_submissions
    ):
        """Test that returned submissions have all expected columns."""
        # Add submissions to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )

        submissions = empty_manager.get_submissions()

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
            assert col_name in submissions.table.column_names


class TestGetSubmissionMembers:
    """Tests for retrieving submission members from the database."""

    def test_get_all_submission_members(self, empty_manager, sample_submission_members):
        """Test getting all submission members."""
        # Add submission members to database
        sample_submission_members.to_sql(
            empty_manager.engine, "submission_members", if_exists="append"
        )

        members = empty_manager.get_submission_members()
        assert len(members) == len(sample_submission_members)

    def test_get_submission_members_by_submission_id(
        self, empty_manager, sample_submission_members
    ):
        """Test getting members for a specific submission."""
        # Add submission members to database
        sample_submission_members.to_sql(
            empty_manager.engine, "submission_members", if_exists="append"
        )

        # Get members for specific submission
        submission_id = sample_submission_members.submission_id[0].as_py()
        members = empty_manager.get_submission_members(submission_ids=[submission_id])

        assert len(members) == len(sample_submission_members)
        assert pc.all(pc.equal(members.submission_id, submission_id)).as_py()

    def test_get_submission_members_empty_database(self, empty_manager):
        """Test getting submission members from an empty database."""
        members = empty_manager.get_submission_members()
        assert len(members) == 0

    def test_get_submission_members_nonexistent_submission(self, empty_manager):
        """Test getting members for a nonexistent submission."""
        members = empty_manager.get_submission_members(submission_ids=["nonexistent"])
        assert len(members) == 0

    def test_get_submission_members_multiple_submissions(self, empty_manager):
        """Test getting members for multiple submissions."""
        from mpcq.submissions.types import SubmissionMembers

        # Create members for two different submissions
        members1 = SubmissionMembers.from_kwargs(
            submission_id=["sub_001"] * 5,
            trksub=["trk_001"] * 5,
            obssubid=[f"obs_{i}" for i in range(5)],
        )
        members2 = SubmissionMembers.from_kwargs(
            submission_id=["sub_002"] * 3,
            trksub=["trk_002"] * 3,
            obssubid=[f"obs_{i}" for i in range(5, 8)],
        )

        # Add to database
        members1.to_sql(empty_manager.engine, "submission_members", if_exists="append")
        members2.to_sql(empty_manager.engine, "submission_members", if_exists="append")

        # Get members for both submissions
        members = empty_manager.get_submission_members(
            submission_ids=["sub_001", "sub_002"]
        )

        assert len(members) == 8
        assert "sub_001" in members.submission_id.to_pylist()
        assert "sub_002" in members.submission_id.to_pylist()

    def test_get_submission_members_returns_correct_columns(
        self, empty_manager, sample_submission_members
    ):
        """Test that returned members have all expected columns."""
        # Add submission members to database
        sample_submission_members.to_sql(
            empty_manager.engine, "submission_members", if_exists="append"
        )

        members = empty_manager.get_submission_members()

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
            assert col_name in members.table.column_names


class TestQueryIntegration:
    """Tests for integrated query operations."""

    def test_get_submissions_and_members_together(
        self, empty_manager, sample_submissions, sample_submission_members
    ):
        """Test getting submissions and their members together."""
        # Add both to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )
        sample_submission_members.to_sql(
            empty_manager.engine, "submission_members", if_exists="append"
        )

        # Get submission
        submission_id = sample_submissions.id[0].as_py()
        submissions = empty_manager.get_submissions(submission_ids=[submission_id])
        members = empty_manager.get_submission_members(submission_ids=[submission_id])

        # Verify relationship
        assert len(submissions) == 1
        assert len(members) > 0
        assert pc.all(pc.equal(members.submission_id, submission_id)).as_py()

    def test_filter_submissions_by_type(self, empty_manager, sample_submissions):
        """Test filtering submissions by type after retrieval."""
        # Add submissions to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )

        # Get all submissions
        submissions = empty_manager.get_submissions()

        # Filter for discovery submissions
        discovery_submissions = submissions.apply_mask(
            pc.equal(submissions.type, "discovery")
        )

        assert len(discovery_submissions) > 0
        assert pc.all(pc.equal(discovery_submissions.type, "discovery")).as_py()

        # Filter for association submissions
        association_submissions = submissions.apply_mask(
            pc.equal(submissions.type, "association")
        )

        assert len(association_submissions) > 0
        assert pc.all(pc.equal(association_submissions.type, "association")).as_py()

    def test_filter_unsubmitted_submissions(self, empty_manager, sample_submissions):
        """Test filtering for unsubmitted submissions."""
        # Add submissions to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )

        # Get all submissions
        submissions = empty_manager.get_submissions()

        # Filter for unsubmitted (submitted_at is null)
        unsubmitted = submissions.apply_mask(pc.is_null(submissions.submitted_at))

        # All sample submissions are unsubmitted
        assert len(unsubmitted) == len(sample_submissions)

    def test_count_observations_per_submission(
        self, empty_manager, sample_submissions, sample_submission_members
    ):
        """Test counting observations per submission."""
        # Add to database
        sample_submissions.to_sql(
            empty_manager.engine, "submissions", if_exists="append"
        )
        sample_submission_members.to_sql(
            empty_manager.engine, "submission_members", if_exists="append"
        )

        submission_id = sample_submissions.id[0].as_py()

        # Get submission and count from metadata
        submission = empty_manager.get_submissions(submission_ids=[submission_id])
        expected_count = submission.observations[0].as_py()

        # Get actual members and count
        members = empty_manager.get_submission_members(submission_ids=[submission_id])
        actual_count = len(members)

        assert actual_count == expected_count
