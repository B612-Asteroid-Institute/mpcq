import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import requests

from .wamo import WAMOResults


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
            Type of object being submitted. Default is "Unclassified".
            Options are: "Unclassified", "Comet", "Asteroid", "Dwarf Planet", "Satellite", "Other".
            This is used to categorize the submission in the MPC database. If not provided, "Unclassified" is used.
            See https://minorplanetcenter.net/submit_psv for more details.

        Returns
        -------
        mpc_submission_id : str
            The MPC submission ID.
        submission_time : datetime
            The time the submission was made.

        Raises
        ------
        ValueError
            If the submission ID is not found.
            If the submission fails.
        """
        if object_type is None:
            object_type = "Unclassified"

        url = "https://minorplanetcenter.net/submit_psv"

        files = {
            "ack": (None, comment),
            "ac2": (None, email),
            "source": (None, open(file, "rb")),
        }

        submission_time = datetime.now().astimezone(timezone.utc)
        response = requests.post(url, files=files)
        self.logger.info(f"Submission response: {response.text}")

        if response.status_code == 200:
            idx = response.text.find("Submission ID is")
            if idx != 1:
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

    def query_wamo(self, requested_values: List[str]) -> WAMOResults:
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

        Returns
        -------
        WAMOResults
            The results of the WAMO API query.
        """
        url = "https://data.minorplanetcenter.net/api/wamo"
        result = requests.get(url, json=requested_values)
        observations = result.json()

        return WAMOResults.from_json(observations)


class MPCSandboxSubmissionClient(MPCSubmissionClient):

    def __init__(self, url: str):
        self.logger = logging.getLogger(__name__)
        self.url = url

    def submit_ades(
        self,
        file: str,
        email: str,
        comment: str,
        object_type: str = "Unclassified",
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
            Type of object being submitted. Default is "Unclassified".
            Options are: "Unclassified", "Comet", "Asteroid", "Dwarf Planet", "Satellite", "Other".
            This is used to categorize the submission in the MPC database. If not provided, "Unclassified" is used.
            See https://minorplanetcenter.net/submit_psv for more details.

        Returns
        -------
        mpc_submission_id : str
            The MPC submission ID.
        submission_time : datetime
            The time the submission was made.

        Raises
        ------
        ValueError
            If the submission ID is not found.
            If the submission fails.
        """
        files = {
            "ack": (None, comment),
            "ac2": (None, email),
            "source": (None, open(file, "rb")),
        }
        submission_time = datetime.now(timezone.utc)
        response = requests.post(os.path.join(self.url, "psv/"), files=files)
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

    def query_wamo(self, requested_values: List[str]) -> WAMOResults:
        pass
