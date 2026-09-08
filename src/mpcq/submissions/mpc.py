import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests

from .wamo import WAMOResults

# --- obj_type: the submit form's REAL radio values ---------------------------
# Transcribed from the verbatim <form> HTML at
# https://minorplanetcenter.net/submit_psv (fetched 2026-08-27). ``obj_type`` is
# a RADIO GROUP, pre-checked at ``unclassified``; the values are LOWERCASE and
# two of them contain a SPACE. The HTML *ids* use underscores
# (``obj_type_neo_candidate``) but the submitted VALUE is ``neo candidate``.
#
# This list previously existed only in a docstring, and it was invented:
# "Unclassified", "Comet", "Asteroid", "Dwarf Planet", "Satellite", "Other" --
# five values the form has never had and one with the wrong case. Worse, the
# field was accepted as a parameter and then never put in the multipart body,
# so every submission this client has ever made went out as the default.
#
# Why it matters: the MPC documents ``obj_type`` as the equivalent of the email
# subject line, i.e. it picks the PROCESSING QUEUE. "Without the correct
# keyword, tracklets could end up in a wrong or slower queue."
# For an association of an already-designated object ``unclassified`` is
# genuinely the right lane, so the old bug was benign; for NEOCP follow-up it
# is fatal to timeliness, which is the entire point of NEOCP follow-up.
OBJ_TYPES = (
    "unclassified",
    "neocp",
    "neo candidate",
    "neo",
    "new comet",
    "comet",
    "tno",
    "artsat",
)

#: The form's pre-checked radio, and the right default for a known object.
DEFAULT_OBJ_TYPE = "unclassified"


def validate_obj_type(object_type: str) -> str:
    """Return ``object_type`` unchanged, or raise ``ValueError``.

    Exact match, deliberately: no case folding and no underscore-to-space
    repair. A caller holding ``"NEOCP"`` or ``"neo_candidate"`` has picked up an
    HTML id or a display label instead of a wire value, and normalizing it
    quietly would hide that until the MPC changes a value and the guesswork
    starts landing in the wrong queue.
    """
    if object_type not in OBJ_TYPES:
        raise ValueError(
            f"Invalid object_type {object_type!r}. Must be one of: "
            + ", ".join(repr(v) for v in OBJ_TYPES)
            + " (lowercase, spaces included)."
        )
    return object_type


class MPCSubmissionClient(ABC):

    @abstractmethod
    def submit_ades():
        pass

    @abstractmethod
    def submit_identifications():
        pass

    @abstractmethod
    def query_wamo():
        pass


class MPCOfficialSubmissionClient(MPCSubmissionClient):

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.submission_url = "https://minorplanetcenter.net/submit_psv"
        self.wamo_url = "https://data.minorplanetcenter.net/api/wamo"

    def submit_ades(
        self,
        file: str,
        email: str,
        comment: str,
        object_type: Optional[str] = None,
    ) -> Tuple[str, datetime]:
        """
        Submit a PSV file to the MPC submission upload form.

        Parameters
        ----------
        file : str
            Path to the PSV file to submit.
        email : str
            Email address of the submitter.
        comment : str
            Comment to include in the submission (this is the acknowledgement contained
            in emailed receipt)
        object_type : str, optional
            The form's ``obj_type`` radio value, which selects the MPC's
            PROCESSING QUEUE (it is the programmatic equivalent of the email
            subject line). Must be one of, exactly, lowercase, spaces included:
            "unclassified", "neocp", "neo candidate", "neo", "new comet",
            "comet", "tno", "artsat". Defaults to "unclassified", which is the
            form's own pre-checked value and the correct lane for astrometry of
            an already-designated object.

            Note that ``obj_type`` alone may not be enough: the MPC documents
            the matching keyword in the ACK text as mandatory for special
            object types ("must have ... in the subject line or ACK") -- e.g.
            "NEOCP" for ``neocp``, "NEO CANDIDATE" for ``neo candidate``.
            Send both. Compose the ``comment`` accordingly.
            See https://minorplanetcenter.net/submit_psv.

        Returns
        -------
        mpc_submission_id : str
            The MPC submission ID.
        submission_time : datetime
            The time the submission was made.

        Raises
        ------
        ValueError
            If ``object_type`` is not one of the form's values.
            If the submission ID is not found.
            If the submission fails.
        """
        if object_type is None:
            object_type = DEFAULT_OBJ_TYPE
        validate_obj_type(object_type)

        # ``obj_type`` MUST be in the multipart body -- it used to be accepted
        # as a parameter here and then dropped on the floor, which silently
        # routed every submission into the default queue.
        submission_time = datetime.now().astimezone(timezone.utc)
        with open(file, "rb") as source:
            files = {
                "ack": (None, comment),
                "ac2": (None, email),
                "obj_type": (None, object_type),
                "source": (None, source),
            }
            response = requests.post(self.submission_url, files=files)
        self.logger.info(f"Submission response: {response.text}")

        if response.status_code == 200:
            idx = response.text.find("Submission ID is")
            # ``!= -1``: the old ``!= 1`` treated "marker not found" (-1) as
            # found and sliced garbage out of the page instead of raising.
            if idx != -1:
                mpc_submission_id = response.text[idx + 17 : idx + 17 + 32]
                return mpc_submission_id, submission_time
            else:
                raise ValueError(f"Submission ID not found: {response.text}")
        else:
            raise ValueError(
                f"Submission failed: {response.text} (status code {response.status_code})"
            )

    def submit_identifications():
        pass

    def query_wamo(
        self, requested_values: List[str], timeout: int = 120
    ) -> WAMOResults:
        """
        Query the WAMO API for the requested values.

        These may take the form of:
            (trksub, stn), ...
            obsid, ...
            obs80, ...
            submission_block_id, ...


        Parameters
        ----------
        requested_values : List[str]
            The values to query the WAMO API for.
        timeout : int, optional
            The timeout for the WAMO API query. Default is 120 seconds.

        Returns
        -------
        WAMOResults
            The results of the WAMO API query.
        """
        result = requests.get(self.wamo_url, json=requested_values, timeout=timeout)
        observations = result.json()

        return WAMOResults.from_json(observations)


class MPCSandboxSubmissionClient(MPCSubmissionClient):

    def __init__(
        self,
        submission_url: str,
        wamo_url: str,
        proxies: Optional[Dict[str, str]] = None,
    ):
        self.logger = logging.getLogger(__name__)
        self.submission_url = submission_url
        self.wamo_url = wamo_url
        self.proxies = proxies

    def submit_ades(
        self,
        file: str,
        email: str,
        comment: str,
        object_type: str = DEFAULT_OBJ_TYPE,
    ) -> Tuple[str, datetime]:
        """
        Submit a PSV file to the MPC submission upload form.

        Parameters
        ----------
        file : str
            Path to the PSV file to submit.
        email : str
            Email address of the submitter.
        comment : str
            Comment to include in the submission (this is the acknowledgement contained
            in emailed receipt)
        object_type : str, optional
            The form's ``obj_type`` radio value -- see
            ``MPCOfficialSubmissionClient.submit_ades``. Must be one of,
            exactly, lowercase, spaces included: "unclassified", "neocp",
            "neo candidate", "neo", "new comet", "comet", "tno", "artsat".
            Defaults to "unclassified".

            The sandbox is ASSUMED to accept the same field set as the
            production form; its exact URL, terms and behaviour are not
            publicly documented, so verify before relying on it.

        Returns
        -------
        mpc_submission_id : str
            The MPC submission ID.
        submission_time : datetime
            The time the submission was made.

        Raises
        ------
        ValueError
            If ``object_type`` is not one of the form's values.
            If the submission ID is not found.
            If the submission fails.
        """
        validate_obj_type(object_type)

        submission_time = datetime.now(timezone.utc)
        with open(file, "rb") as source:
            files = {
                "ack": (None, comment),
                "ac2": (None, email),
                "obj_type": (None, object_type),
                "source": (None, source),
            }
            response = requests.post(
                urljoin(self.submission_url, "psv/"),
                files=files,
            )
        self.logger.info(f"Submission response: {response.text}")

        if response.status_code == 200:
            idx = response.text.find("MPC submission ID : ")
            if idx != -1:
                mpc_submission_id = response.text[idx + 28 : idx + 28 + 32]
                return mpc_submission_id, submission_time
            else:
                raise ValueError(f"Submission ID not found: {response.text}")
        else:
            raise ValueError(
                f"Submission failed: {response.text} (status code {response.status_code})"
            )

    def submit_identifications(self):
        pass

    def query_wamo(
        self, requested_values: List[str], timeout: int = 120
    ) -> WAMOResults:
        """
        Query the WAMO API for the requested values.

        The requested values may take the form of:
            (trksub, stn), ...
            obsid, ...
            obs80, ...
            submission_block_id, ...

        Parameters
        ----------
        requested_values : List[str]
            The values to query the WAMO API for.
        timeout : int, optional
            The timeout for the WAMO API query. Default is 120 seconds.

        Returns
        -------
        WAMOResults
            The results of the WAMO API query.
        """
        result = requests.get(
            self.wamo_url, json=requested_values, proxies=self.proxies, timeout=timeout
        )
        self.logger.info(f"WAMO API response: {result.text}")
        observations = result.json()

        return WAMOResults.from_json(observations)
