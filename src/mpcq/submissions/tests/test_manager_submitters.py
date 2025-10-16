"""
Tests for SubmissionManager submitter management.
"""

from datetime import datetime, timezone
from unittest.mock import patch

import pyarrow.compute as pc
import pytest

from mpcq.submissions.types import Submitter


class TestAddSubmitter:
    """Tests for adding submitters to the database."""

    def test_add_submitter(self, empty_manager, sample_submitter):
        """Test adding a new submitter."""
        empty_manager.add_submitter(sample_submitter)

        # Retrieve the submitter
        submitters = empty_manager.get_submitters()
        assert len(submitters) == 1
        assert submitters.first_name[0].as_py() == sample_submitter.first_name
        assert submitters.last_name[0].as_py() == sample_submitter.last_name
        assert submitters.email[0].as_py() == sample_submitter.email
        assert submitters.institution[0].as_py() == sample_submitter.institution

    def test_add_multiple_submitters(self, empty_manager):
        """Test adding multiple submitters."""
        submitter1 = Submitter(
            first_name="Alice",
            last_name="Smith",
            email="alice@example.com",
            institution="University A",
        )
        submitter2 = Submitter(
            first_name="Bob",
            last_name="Jones",
            email="bob@example.com",
            institution="University B",
        )

        empty_manager.add_submitter(submitter1)
        empty_manager.add_submitter(submitter2)

        submitters = empty_manager.get_submitters()
        assert len(submitters) == 2

        # Check that both were added
        emails = submitters.email.to_pylist()
        assert "alice@example.com" in emails
        assert "bob@example.com" in emails

    def test_add_duplicate_submitter(self, empty_manager, sample_submitter):
        """Test that adding a duplicate submitter doesn't create a new entry."""
        empty_manager.add_submitter(sample_submitter)
        empty_manager.add_submitter(sample_submitter)

        # Should still only have one submitter
        submitters = empty_manager.get_submitters()
        assert len(submitters) == 1

    def test_add_submitter_without_institution(self, empty_manager):
        """Test adding a submitter without an institution."""
        submitter = Submitter(
            first_name="Test",
            last_name="User",
            email="test@example.com",
            institution=None,
        )

        empty_manager.add_submitter(submitter)

        submitters = empty_manager.get_submitters()
        assert len(submitters) == 1
        assert submitters.institution[0].as_py() is None

    def test_submitter_id_auto_increments(self, empty_manager):
        """Test that submitter IDs auto-increment correctly."""
        submitter1 = Submitter(
            first_name="First",
            last_name="User",
            email="first@example.com",
            institution="Inst1",
        )
        submitter2 = Submitter(
            first_name="Second",
            last_name="User",
            email="second@example.com",
            institution="Inst2",
        )

        empty_manager.add_submitter(submitter1)
        empty_manager.add_submitter(submitter2)

        submitters = empty_manager.get_submitters()
        ids = sorted(submitters.id.to_pylist())
        assert ids == [1, 2]


class TestGetSubmitters:
    """Tests for retrieving submitters from the database."""

    def test_get_all_submitters(self, empty_manager, sample_submitters_table):
        """Test getting all submitters."""
        # Add submitters to database
        sample_submitters_table.to_sql(
            empty_manager.engine, "submitters", if_exists="append"
        )

        submitters = empty_manager.get_submitters()
        assert len(submitters) == len(sample_submitters_table)

    def test_get_submitters_by_id(self, empty_manager, sample_submitters_table):
        """Test getting specific submitters by ID."""
        # Add submitters to database
        sample_submitters_table.to_sql(
            empty_manager.engine, "submitters", if_exists="append"
        )

        # Get submitter with ID 1
        submitters = empty_manager.get_submitters(submitter_ids=[1])
        assert len(submitters) == 1
        assert submitters.id[0].as_py() == 1
        assert submitters.email[0].as_py() == "test@example.com"

    def test_get_submitters_by_multiple_ids(
        self, empty_manager, sample_submitters_table
    ):
        """Test getting multiple submitters by ID."""
        # Add submitters to database
        sample_submitters_table.to_sql(
            empty_manager.engine, "submitters", if_exists="append"
        )

        # Get submitters with IDs 1 and 2
        submitters = empty_manager.get_submitters(submitter_ids=[1, 2])
        assert len(submitters) == 2

        ids = submitters.id.to_pylist()
        assert 1 in ids
        assert 2 in ids

    def test_get_submitters_empty_database(self, empty_manager):
        """Test getting submitters from an empty database."""
        submitters = empty_manager.get_submitters()
        assert len(submitters) == 0

    def test_get_nonexistent_submitter(self, empty_manager):
        """Test getting a submitter that doesn't exist."""
        submitters = empty_manager.get_submitters(submitter_ids=[999])
        assert len(submitters) == 0


class TestSelectSubmitter:
    """Tests for the interactive submitter selection."""

    def test_select_existing_submitter(self, empty_manager, sample_submitters_table):
        """Test selecting an existing submitter interactively."""
        # Add submitters to database
        sample_submitters_table.to_sql(
            empty_manager.engine, "submitters", if_exists="append"
        )

        # Mock user input to select the first submitter (choice 1)
        with patch("builtins.input", return_value="1"):
            empty_manager.select_submitter()

        # Check that the submitter was selected
        assert empty_manager.submitter is not None
        assert empty_manager.submitter.id == 1
        assert empty_manager.submitter.email == "test@example.com"

    def test_select_submitter_choice_two(self, empty_manager, sample_submitters_table):
        """Test selecting the second submitter."""
        # Add submitters to database
        sample_submitters_table.to_sql(
            empty_manager.engine, "submitters", if_exists="append"
        )

        # Mock user input to select the second submitter (choice 2)
        with patch("builtins.input", return_value="2"):
            empty_manager.select_submitter()

        # Check that the second submitter was selected
        assert empty_manager.submitter is not None
        assert empty_manager.submitter.id == 2
        assert empty_manager.submitter.email == "another@example.com"

    def test_select_new_submitter(self, empty_manager, sample_submitters_table):
        """Test creating a new submitter via selection."""
        # Add submitters to database
        sample_submitters_table.to_sql(
            empty_manager.engine, "submitters", if_exists="append"
        )

        # Mock user input: 0 to add new, then submitter details, then confirm
        inputs = [
            "0",  # Choose to add new submitter
            "New",  # first_name
            "Submitter",  # last_name
            "new@example.com",  # email
            "New Institution",  # institution
            "y",  # confirm
        ]
        with patch("builtins.input", side_effect=inputs):
            empty_manager.select_submitter()

        # Check that the new submitter was created and selected
        assert empty_manager.submitter is not None
        assert empty_manager.submitter.first_name == "New"
        assert empty_manager.submitter.last_name == "Submitter"
        assert empty_manager.submitter.email == "new@example.com"
        assert empty_manager.submitter.institution == "New Institution"

        # Verify it was added to the database
        submitters = empty_manager.get_submitters()
        assert len(submitters) == 3  # Original 2 + 1 new

    def test_select_new_submitter_without_institution(
        self, empty_manager, sample_submitters_table
    ):
        """Test creating a new submitter without institution."""
        # Add submitters to database
        sample_submitters_table.to_sql(
            empty_manager.engine, "submitters", if_exists="append"
        )

        # Mock user input: 0 to add new, empty institution, then confirm
        inputs = [
            "0",  # Choose to add new submitter
            "Jane",  # first_name
            "Doe",  # last_name
            "jane@example.com",  # email
            "",  # institution (empty)
            "y",  # confirm
        ]
        with patch("builtins.input", side_effect=inputs):
            empty_manager.select_submitter()

        # Check that institution is None
        assert empty_manager.submitter.institution is None

    def test_select_submitter_retry_on_invalid_choice(
        self, empty_manager, sample_submitters_table
    ):
        """Test that invalid choices prompt retry."""
        # Add submitters to database
        sample_submitters_table.to_sql(
            empty_manager.engine, "submitters", if_exists="append"
        )

        # Mock user input: invalid choice, then valid choice
        inputs = [
            "99",  # Invalid choice
            "1",  # Valid choice
        ]
        with patch("builtins.input", side_effect=inputs):
            empty_manager.select_submitter()

        # Should have selected submitter 1
        assert empty_manager.submitter.id == 1

    def test_select_submitter_when_none_exist(self, empty_manager):
        """Test selecting a submitter when database is empty."""
        # Mock user input to create a new submitter
        inputs = [
            "First",  # first_name
            "User",  # last_name
            "first@example.com",  # email
            "First Institution",  # institution
            "y",  # confirm
        ]
        with patch("builtins.input", side_effect=inputs):
            empty_manager.select_submitter()

        # Check that the new submitter was created
        assert empty_manager.submitter is not None
        assert empty_manager.submitter.first_name == "First"

        # Verify it was added to the database
        submitters = empty_manager.get_submitters()
        assert len(submitters) == 1

    def test_prompt_new_submitter_retry_on_rejection(self, empty_manager):
        """Test that rejecting a new submitter prompts retry."""
        # Mock user input: reject first entry, accept second
        inputs = [
            "Wrong",  # first_name (first attempt)
            "Name",  # last_name
            "wrong@example.com",  # email
            "",  # institution
            "n",  # reject
            "Right",  # first_name (second attempt)
            "Name",  # last_name
            "right@example.com",  # email
            "Right Institution",  # institution
            "y",  # accept
        ]
        with patch("builtins.input", side_effect=inputs):
            empty_manager._prompt_new_submitter()

        # Check that the second (accepted) submitter was created
        assert empty_manager.submitter.email == "right@example.com"


class TestSubmitterProperty:
    """Tests for the submitter property behavior."""

    def test_submitter_starts_none(self, empty_manager):
        """Test that submitter starts as None."""
        assert empty_manager.submitter is None

    def test_set_submitter_directly(self, empty_manager, sample_submitter):
        """Test setting the submitter directly."""
        empty_manager._submitter = sample_submitter
        assert empty_manager.submitter == sample_submitter

    def test_delete_submitter(self, manager_with_submitter):
        """Test deleting the submitter."""
        assert manager_with_submitter.submitter is not None

        del manager_with_submitter.submitter

        assert manager_with_submitter.submitter is None
