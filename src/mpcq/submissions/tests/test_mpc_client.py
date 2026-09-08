"""Tests for the MPC submission clients' multipart body and obj_type handling.

Background: ``submit_ades`` accepted an ``object_type`` argument, validated
nothing, and then built a multipart body of ``{ack, ac2, source}`` -- dropping
the field entirely. Every submission this client has ever made therefore went
out as the form's default queue, and the docstring advertised six ``obj_type``
values that do not exist on the form ("Comet", "Asteroid", "Dwarf Planet",
"Satellite", "Other", and "Unclassified" with the wrong case).

``obj_type`` selects the MPC's PROCESSING QUEUE, so these tests pin (a) that
the field is in the body, (b) its exact wire spelling, and (c) that a bad value
fails here rather than being silently accepted and mis-routed.

No network: ``requests.post`` is mocked in every test.
"""

import tempfile
from unittest.mock import MagicMock, patch

import pytest

from mpcq.submissions.mpc import (
    DEFAULT_OBJ_TYPE,
    OBJ_TYPES,
    MPCOfficialSubmissionClient,
    MPCSandboxSubmissionClient,
    validate_obj_type,
)


@pytest.fixture
def psv_file():
    """A throwaway file for ``submit_ades`` to open."""
    with tempfile.NamedTemporaryFile("w", suffix=".psv", delete=False) as handle:
        handle.write("# version=2022\n")
        return handle.name


@pytest.fixture
def ok_response():
    """A 200 whose body satisfies BOTH clients' id parsers.

    They look for different marker strings ("Submission ID is" vs
    "MPC submission ID : ") at different offsets, so the body carries both --
    these tests are about the request, not the response parsing, and an
    unparseable body would raise before the request could be inspected.
    """
    response = MagicMock()
    response.status_code = 200
    response.text = (
        "MPC submission ID : " + "b" * 40 + "\nSubmission ID is " + "a" * 32
    )
    return response


@pytest.fixture(params=["official", "sandbox"])
def client(request):
    """Both clients: they had the identical bug and must have the identical fix."""
    if request.param == "official":
        return MPCOfficialSubmissionClient()
    return MPCSandboxSubmissionClient(
        submission_url="https://sandbox.invalid/", wamo_url="https://sandbox.invalid/wamo"
    )


class TestObjTypeIsSent:
    def test_obj_type_is_in_the_multipart_body(self, client, psv_file, ok_response):
        with patch("mpcq.submissions.mpc.requests.post", return_value=ok_response) as post:
            client.submit_ades(psv_file, "a@b.org", "ack text", object_type="neocp")

        files = post.call_args.kwargs["files"]
        assert "obj_type" in files, "obj_type was dropped from the multipart body"
        assert files["obj_type"] == (None, "neocp")
        # The pre-existing fields are untouched.
        assert files["ack"] == (None, "ack text")
        assert files["ac2"] == (None, "a@b.org")
        assert "source" in files

    def test_the_default_is_the_forms_lowercase_prechecked_value(
        self, client, psv_file, ok_response
    ):
        with patch("mpcq.submissions.mpc.requests.post", return_value=ok_response) as post:
            client.submit_ades(psv_file, "a@b.org", "ack text")

        assert post.call_args.kwargs["files"]["obj_type"] == (None, "unclassified")
        assert DEFAULT_OBJ_TYPE == "unclassified"

    def test_every_real_value_round_trips_verbatim(self, client, psv_file, ok_response):
        for value in OBJ_TYPES:
            with patch(
                "mpcq.submissions.mpc.requests.post", return_value=ok_response
            ) as post:
                client.submit_ades(psv_file, "a@b.org", "ack", object_type=value)
            assert post.call_args.kwargs["files"]["obj_type"] == (None, value)

    def test_an_explicit_none_falls_back_to_the_default(
        self, psv_file, ok_response
    ):
        """Only the official client takes ``Optional[str]``; the sandbox one has
        a real default in its signature."""
        client = MPCOfficialSubmissionClient()
        with patch("mpcq.submissions.mpc.requests.post", return_value=ok_response) as post:
            client.submit_ades(psv_file, "a@b.org", "ack", object_type=None)
        assert post.call_args.kwargs["files"]["obj_type"] == (None, DEFAULT_OBJ_TYPE)


class TestObjTypeIsValidated:
    def test_an_invalid_value_raises_before_anything_is_posted(
        self, client, psv_file
    ):
        with patch("mpcq.submissions.mpc.requests.post") as post:
            with pytest.raises(ValueError, match="Invalid object_type"):
                client.submit_ades(psv_file, "a@b.org", "ack", object_type="Asteroid")
        post.assert_not_called()

    def test_the_invented_docstring_values_all_raise(self, client, psv_file):
        for bogus in (
            "Unclassified",
            "Comet",
            "Asteroid",
            "Dwarf Planet",
            "Satellite",
            "Other",
        ):
            with pytest.raises(ValueError):
                client.submit_ades(psv_file, "a@b.org", "ack", object_type=bogus)


class TestValidateObjType:
    def test_the_eight_values_are_the_forms_own(self):
        assert OBJ_TYPES == (
            "unclassified",
            "neocp",
            "neo candidate",
            "neo",
            "new comet",
            "comet",
            "tno",
            "artsat",
        )

    def test_valid_values_pass_through_unchanged(self):
        for value in OBJ_TYPES:
            assert validate_obj_type(value) == value

    def test_no_case_folding_or_underscore_repair(self):
        """An uppercase or underscored value means an HTML id or a display
        label leaked into the wire value -- a bug worth surfacing, not
        smoothing over."""
        for value in ("NEOCP", "Neocp", "neo_candidate", "NEO CANDIDATE"):
            with pytest.raises(ValueError):
                validate_obj_type(value)

    def test_the_error_lists_the_legal_values(self):
        with pytest.raises(ValueError, match="neo candidate"):
            validate_obj_type("bogus")


def test_official_client_raises_when_submission_id_marker_missing(tmp_path, monkeypatch):
    """A 200 whose body lacks the 'Submission ID is' marker must RAISE -- the
    old ``idx != 1`` check treated find()'s -1 as success and sliced garbage."""
    from unittest.mock import MagicMock, patch

    from mpcq.submissions.mpc import MPCOfficialSubmissionClient

    f = tmp_path / "obs.psv"
    f.write_text("# version=2022\n")
    client = MPCOfficialSubmissionClient()
    resp = MagicMock(status_code=200, text="<html>thanks, but no id here</html>")
    with patch("mpcq.submissions.mpc.requests.post", return_value=resp):
        try:
            client.submit_ades(str(f), comment="x", email="y@z.org")
        except ValueError as e:
            assert "Submission ID not found" in str(e)
        else:
            raise AssertionError("expected ValueError on marker-less 200")
