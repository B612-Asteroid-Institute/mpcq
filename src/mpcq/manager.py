import datetime
import json
import logging
import os
import sys
import time
import warnings
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import quivr as qv
import requests
import sqlalchemy as sq
from adam_core.observations import SourceCatalog
from adam_core.observations.ades import ADES_to_string, ObsContext
from adam_core.orbit_determination import FittedOrbitMembers, FittedOrbits

from .client import BigQueryMPCClient, MPCClient
from .identifications import identifications_to_json_string
from .submissions import (
    MPCSubmissionResults,
    SubmissionMembers,
    Submissions,
    prepare_submission,
)


def round_to_nearest_millisecond(t: datetime.datetime) -> datetime.datetime:
    microseconds = np.ceil(t.microsecond / 1000).astype(int) * 1000
    return t.replace(microsecond=microseconds)


def submit_new_observations(
    file, email, comment, dry_run: bool = True
) -> Tuple[str, datetime.datetime]:
    """
    Submit new observations to the Minor Planet Center.

    Parameters
    ----------
    file : str
        Path to the file containing the new observations in PSV format.
    email : str
        Email address of the submitter.
    comment : str
        Acknowledgement comment
    dry_run : bool
        If True, the submission is a dry run and no submission is made to the MPC.

    Returns
    -------
    submission_id : str
        MPC-assigned Submission ID. If no submission is made, the submission ID is "".
    submission_time : datetime.datetime
        Time of the submission.
    """
    if dry_run:
        url = "https://minorplanetcenter.net/submit_psv_test"
    else:
        url = "https://minorplanetcenter.net/submit_psv"

    files = {
        "ack": (None, comment),
        "ac2": (None, email),
        "source": (None, open(file, "rb")),
    }

    response = requests.post(url, files=files)
    submission_time = datetime.datetime.now().astimezone(datetime.timezone.utc)

    if dry_run:
        if response.text == "Submission format valid.\n":
            return "", submission_time
        else:
            raise ValueError(
                f"Submission failed: {response.text} (status code {response.status_code}) (dry run)"
            )
    else:
        if response.status_code == 200:
            idx = response.text.find("Submission ID is")
            mpc_submission_id = response.text[idx + 17 : idx + 17 + 32]
            return mpc_submission_id, submission_time
        else:
            raise ValueError(
                f"Submission failed: {response.text} (status code {response.status_code})"
            )


def submit_identifications(file: str, dry_run: bool = True) -> datetime.datetime:
    """
    Submit identifications to the Minor Planet Center's ID pipeline.

    Parameters
    ----------
    file : str
        Path to the file containing the identifications in JSON format.
    dry_run : bool
        If True, the submission is a dry run and no submission is made to the MPC.

    Returns
    -------
    submission_time : datetime.datetime
        Time of the submission.
    """
    url = "https://minorplanetcenter.net/mpcops/submissions/identifications/"
    with open(file, "r") as json_file:
        json_data = json.load(json_file)

    if not dry_run:
        response = requests.get(url, json=json_data)
        submission_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
        if (
            response.text
            == '{"message": "Thank you for submitting your identifications. This message acknowledges receipt of your data."}'
        ):
            return submission_time
        else:
            raise ValueError(
                f"Submission failed: {response.text} (status code {response.status_code})"
            )

    else:
        response = None
        submission_time = datetime.datetime.now().astimezone(datetime.timezone.utc)

    return submission_time


class MPCCrossmatch(qv.Table):
    obs_id = qv.LargeStringColumn()
    mpc_id = qv.LargeStringColumn()
    time_difference = qv.Float64Column()
    distance = qv.Float64Column()
    status = qv.LargeStringColumn(nullable=True)
    trksub = qv.LargeStringColumn(nullable=True)


@dataclass
class Submitter:
    first_name: str
    last_name: str
    email: str


class SubmissionManager:

    def __init__(
        self, engine: sq.engine.base.Engine, metadata: sq.MetaData, directory: str
    ):
        self.engine = engine
        self.metadata = metadata
        self.tables = metadata.tables
        self.directory = directory
        self.submission_directory = os.path.join(directory, "submissions")
        self.submitter = None

        self.setup_logging()

    def setup_logging(self) -> None:
        """
        Setup logging for the SubmissionManager.

        Returns
        -------
        None
        """
        logger = logging.getLogger("SubmissionManager")
        logger.setLevel(logging.INFO)

        console_handler = logging.StreamHandler(sys.stdout)
        stream_formatter = logging.Formatter(
            "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(stream_formatter)
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)

        file_handler = logging.FileHandler(
            os.path.join(self.directory, "manager.log"),
            mode="a",
            encoding="utf-8",
            delay=False,
        )
        file_formatter = logging.Formatter(
            "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s (%(filename)s, %(funcName)s, %(lineno)d)",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

        logger.info("SubmissionManager initialized.")
        logger.info(
            f"Database located at {os.path.join(self.directory, 'tracking.db')}"
        )
        self.logger = logger

        return

    def connect_client(self, client: Optional[MPCClient] = None) -> None:
        """
        Connect the SubmissionManager to an MPCClient.

        Parameters
        ----------
        client : MPCClient, optional
            The client to connect to. If None, a BigQueryMPCClient will be used.

        Returns
        -------
        None
        """
        if client is None:
            client = BigQueryMPCClient()

        self.client = client

    def set_submitter(self, first_name: str, last_name: str, email: str) -> None:
        """
        Set the user submitting the observations.

        Parameters
        ----------
        first_name : str
            The first name of the submitter. E.g. "John".
        last_name : str
            The last name of the submitter. E.g. "Doe".
        email : str
            The email address of the submitter. E.g. "john.doe@university.edu"

        Returns
        -------
        None
        """
        self.submitter = Submitter(first_name, last_name, email)
        self.logger.info(f"Submitter set to {first_name} {last_name} ({email}).")

    @classmethod
    def create(cls, directory: str) -> "SubmissionManager":
        """
        Create a new SubmissionManager instance.

        Parameters
        ----------
        directory : str
            The directory to store the database and related files.

        Returns
        -------
        SubmissionManager
            The new SubmissionManager instance.
        """
        os.makedirs(directory, exist_ok=True)
        submission_directory = os.path.join(directory, "submissions")
        os.makedirs(submission_directory, exist_ok=True)

        if os.path.exists(os.path.join(directory, "tracking.db")):
            raise FileExistsError("A database already exists in this directory.")

        engine = sq.create_engine("sqlite:///" + os.path.join(directory, "tracking.db"))

        metadata = sq.MetaData()
        sq.Table(
            "submission_members",
            metadata,
            sq.Column("submission_id", sq.String),
            sq.Column("orbit_id", sq.String),
            sq.Column("trksub", sq.String),
            sq.Column("obssubid", sq.String, primary_key=True),
            sq.Column("deep_drilling_filtered", sq.Boolean),
            sq.Column("itf_obs_id", sq.String, nullable=True),
            sq.Column("submitted", sq.Boolean),
            sq.UniqueConstraint("orbit_id", "obssubid", name="uc_orbit_obssubid"),
        )

        sq.Table(
            "submissions",
            metadata,
            sq.Column("id", sq.String, primary_key=True),
            sq.Column("mpc_submission_id", sq.String, nullable=True),
            sq.Column("orbits", sq.Integer),
            sq.Column("observations", sq.Integer),
            sq.Column("observations_submitted", sq.Integer),
            sq.Column("deep_drilling_observations", sq.Integer),
            sq.Column("new_observations", sq.Integer),
            sq.Column("new_observations_file", sq.String, nullable=True),
            sq.Column("new_observations_submitted", sq.Boolean),
            sq.Column("new_observations_submitted_at", sq.DateTime, nullable=True),
            sq.Column("itf_observations", sq.Integer),
            sq.Column("itf_identifications_file", sq.String, nullable=True),
            sq.Column("itf_identifications_submitted", sq.Boolean),
            sq.Column("itf_identifications_submitted_at", sq.DateTime, nullable=True),
        )

        metadata.create_all(engine)

        return cls(engine, metadata, directory)

    @classmethod
    def from_dir(cls, directory: str) -> "SubmissionManager":
        """
        Load a SubmissionManager instance from an existing directory.

        Parameters
        ----------
        directory : str
            The directory containing the database and related files.

        Returns
        -------
        SubmissionManager
            The SubmissionManager instance.
        """
        tracking_db = os.path.join(directory, "tracking.db")
        if not os.path.exists(tracking_db):
            raise FileNotFoundError(
                "No database found in this directory. Use create method to create a new database."
            )

        engine = sq.create_engine("sqlite:///" + tracking_db)

        metadata = sq.MetaData()
        metadata.reflect(bind=engine)

        return cls(engine, metadata, os.path.abspath(directory))

    def get_submission_members(
        self, submission_ids: Optional[list[str]] = None
    ) -> SubmissionMembers:
        """
        Retrieve the submission members tracked in the database.

        Parameters
        ----------
        submission_ids : list[str], optional
            The submission_ids to retrieve. If None, all submission members are
            returned.

        Returns
        -------
        SubmissionMembers
            The submission members.
        """
        stmt = sq.select(self.tables["submission_members"])
        if submission_ids is not None:
            stmt = stmt.where(
                self.tables["submission_members"].c.submission_id.in_(submission_ids)
            )

        return SubmissionMembers.from_sql(
            self.engine,
            self.tables["submission_members"],
            statement=stmt,
            chunk_size=10000,
        )

    def get_submissions(self) -> Submissions:
        """
        Retrieve the submissions tracked in the database.

        Returns
        -------
        Submissions
            The submissions with a breakdown of the number of orbits, observations, and how
            many new vs ITF observations have been submitted.
        """
        return Submissions.from_sql(self.engine, self.tables["submissions"])

    def query_mpc_submission_results(
        self,
        mpc_submission_ids: Optional[list[str]] = None,
        submission_ids: Optional[list[str]] = None,
    ) -> MPCSubmissionResults:
        """
        Query the MPC for the results of the submissions.

        Parameters
        ----------
        mpc_submission_ids : list[str]
            The submission_ids to query (submission IDs assigned by the MPC).
        submission_ids : list[str]
            The submission_ids to query (submission IDs created by the user).
        """
        if mpc_submission_ids is not None and submission_ids is not None:
            warnings.warn(
                "Both mpc_submission_ids and submission_ids were provided. Only mpc_submission_ids will be used."
            )
            submission_ids = None

        if submission_ids is not None:
            submissions = self.get_submissions()
            mpc_submission_ids = submissions.apply_mask(
                pc.is_in(submissions.id, pa.array(submission_ids))
            ).mpc_submission_id.to_pylist()

            results = self.client.query_submission_results(mpc_submission_ids)

        if mpc_submission_ids is None:
            submissions = self.get_submissions()
            submissions_with_mpc_id = submissions.apply_mask(
                pc.invert(pc.is_null(submissions.mpc_submission_id))
            )
            mpc_submission_ids = submissions_with_mpc_id.mpc_submission_id.to_pylist()

            results = self.client.query_submission_results(mpc_submission_ids)

        return results

    def prepare_submission(
        self,
        submission_id: str,
        orbits: FittedOrbits,
        orbit_members: FittedOrbitMembers,
        observations: SourceCatalog,
        mpc_crossmatch: MPCCrossmatch,
        obs_contexts: dict[str, ObsContext],
        identifications_comment: Optional[str] = None,
        max_obs_per_night: int = 6,
        astrometric_catalog: Optional[SourceCatalog] = None,
    ) -> Submissions:
        """
        Prepare a submission to the minor planet center. This will save the submission to the
        database and return the submission object.

        Parameters
        ----------
        submission_id : str
            The internal submission ID to use.
        orbits : FittedOrbits
            The orbits to submit.
        orbit_members : FittedOrbitMembers
            The orbit members to submit.
        observations : SourceCatalog
            The observations from which the orbits were derived.
        mpc_crossmatch : MPCCrossmatch
            The crossmatch between the observations and the MPC.
        obs_contexts : dict[str, ObsContext]
            The observing contexts which will be used to create the ADES files.
        identifications_comment : str, optional
            A comment to include in the ITF identifications file.
        max_obs_per_night : int, optional
            The maximum number of observations to submit per night.
        astrometric_catalog : SourceCatalog, optional
            The astrometric catalog to use for the observations.

        Returns
        -------
        Submissions
            The submission object which contains the submission ID and paths to the files
            on disk.
        """
        if self.submitter is None:
            raise ValueError(
                "User must be set before submitting observations. See set_submitter method."
            )

        # Check if the submission ID already exists
        submissions = self.get_submissions()
        if submission_id in submissions.id.to_pylist():
            raise ValueError(f"Submission ID {submission_id} already exists.")

        submission_members, new_observations, identifications = prepare_submission(
            submission_id,
            orbits,
            orbit_members,
            observations,
            max_obs_per_night=max_obs_per_night,
            mpc_crossmatch=mpc_crossmatch,
            astrometric_catalog=astrometric_catalog,
        )

        new_observations_file_base = f"{submission_id}.psv"
        itf_identifications_file_base = f"{submission_id}_identifications.json"

        if len(new_observations) > 0:
            new_observations_file = os.path.abspath(
                os.path.join(self.directory, "submissions", new_observations_file_base)
            )
        else:
            new_observations_file = None

        if len(identifications.itf()) > 0:
            itf_identifications_file = os.path.abspath(
                os.path.join(
                    self.directory, "submissions", itf_identifications_file_base
                )
            )

            if os.path.exists(itf_identifications_file):
                raise FileExistsError(
                    f"ITF identifications file {itf_identifications_file} already exists."
                )

            if identifications_comment is None:
                identifications_comment = ""

            json_str = identifications_to_json_string(
                identifications,
                orbits,
                f"{self.submitter.first_name[0]}. {self.submitter.last_name}",
                self.submitter.email,
                identifications_comment,
            )

            with open(itf_identifications_file, "w") as f:
                f.write(json_str)
        else:
            itf_identifications_file = None

        if new_observations_file is not None:

            if os.path.exists(new_observations_file):
                raise FileExistsError(
                    f"New observations file {new_observations_file} already exists."
                )

            ades_str = ADES_to_string(
                new_observations,
                obs_contexts,
                seconds_precision=3,
                columns_precision={
                    "ra": 8,
                    "dec": 8,
                    "mag": 2,
                    "rmsRA": 4,
                    "rmsDec": 4,
                    "rmsMag": 2,
                },
            )

            with open(new_observations_file, "w") as f:
                f.write(ades_str)

        submission = Submissions.from_kwargs(
            id=[submission_id],
            mpc_submission_id=None,
            orbits=[len(orbits)],
            observations=[len(submission_members)],
            observations_submitted=[
                len(submission_members.select("deep_drilling_filtered", False))
            ],
            deep_drilling_observations=[
                len(submission_members.select("deep_drilling_filtered", True))
            ],
            new_observations=[len(new_observations)],
            new_observations_file=[new_observations_file],
            new_observations_submitted=[False],
            new_observations_submitted_at=[None],
            itf_observations=[len(identifications.itf())],
            itf_identifications_file=[itf_identifications_file],
            itf_identifications_submitted=[False],
            itf_identifications_submitted_at=[None],
        )

        try:
            submission.to_sql(self.engine, "submissions")
            submission_members.to_sql(self.engine, "submission_members")
            self.logger.info(
                f"Submission {submission_id} and {len(submission_members)} submission members saved to the database."
            )
        except Exception:
            self.logger.critical(
                f"Submission {submission_id} could not be saved to the database."
            )
            raise ValueError("Submission could not be saved to the database.")

        return submission

    def delete_prepared_submission(self, submission_id: str) -> None:
        """
        Delete a submission that has been prepared but not yet submitted.

        Parameters
        ----------
        submission_id : str
            The submission ID to delete.

        Returns
        -------
        None
        """
        # Get the files for the submission
        submissions = self.get_submissions()
        submission = submissions.select("id", submission_id)
        if len(submission) == 0:
            raise ValueError(f"Submission {submission_id} not found in the database.")

        if (
            submission.new_observations_submitted[0].as_py()
            or submission.itf_identifications_submitted[0].as_py()
        ):
            raise ValueError(f"Submission {submission_id} has already been submitted.")

        new_observations_file = submission.new_observations_file[0].as_py()
        itf_identifications_file = submission.itf_identifications_file[0].as_py()

        if new_observations_file is not None and os.path.exists(new_observations_file):
            os.remove(new_observations_file)
            self.logger.info(f"New observations file {new_observations_file} deleted.")

        if itf_identifications_file is not None and os.path.exists(
            itf_identifications_file
        ):
            os.remove(itf_identifications_file)
            self.logger.info(
                f"ITF identifications file {itf_identifications_file} deleted."
            )

        # Now, delete the submission and submission members from the database
        with self.engine.begin() as conn:

            stmt = sq.delete(self.tables["submissions"]).where(
                self.tables["submissions"].c.id == submission_id
            )

            conn.execute(stmt)

            stmt = sq.delete(self.tables["submission_members"]).where(
                self.tables["submission_members"].c.submission_id == submission_id
            )

            conn.execute(stmt)

        self.logger.info(f"Submission {submission_id} deleted from the database.")

        return

    def label_new_observations_submitted(
        self,
        submission_id: str,
        mpc_submission_id: str,
        submitted_at: datetime.datetime,
    ) -> None:
        """
        Label the new observations as submitted to the MPC.

        Parameters
        ----------
        submission_id : str
            The submission ID.
        mpc_submission_id : str
            The MPC submission ID.
        submitted_at : datetime
            The time at which the submission was made.

        Returns
        -------
        None
        """
        # Insure datetime is in UTC and rounded to the nearest millisecond
        submitted_at = submitted_at.astimezone(datetime.timezone.utc)
        submitted_at = round_to_nearest_millisecond(submitted_at)

        # Check current status and raise an error if already submitted
        with self.engine.begin() as conn:

            stmt = sq.select(
                self.tables["submissions"].c.new_observations_submitted
            ).where(self.tables["submissions"].c.id == submission_id)

            result = conn.execute(stmt).fetchone()[0]

            if result:
                raise ValueError(
                    f"New observations for submission {submission_id} have already been marked as submitted."
                )

        # Now, update the submission to mark the new observations as submitted
        with self.engine.begin() as conn:

            stmt = (
                sq.update(self.tables["submissions"])
                .where(self.tables["submissions"].c.id == submission_id)
                .values(
                    new_observations_submitted=True,
                    new_observations_submitted_at=submitted_at,
                    mpc_submission_id=mpc_submission_id,
                )
            )

            conn.execute(stmt)

            self.logger.info(
                f"New observations for submission {submission_id} marked as submitted."
            )

            stmt = (
                sq.update(self.tables["submission_members"])
                .where(
                    sq.and_(
                        self.tables["submission_members"].c.submission_id
                        == submission_id,
                        self.tables["submission_members"].c.itf_obs_id.is_(None),
                        self.tables["submission_members"].c.deep_drilling_filtered.is_(
                            False
                        ),
                    )
                )
                .values(submitted=True)
            )

            conn.execute(stmt)

            self.logger.info(
                f"Submission members for submission {submission_id} marked as submitted."
            )

        return

    def label_identifications_submitted(
        self, submission_id: str, submitted_at: datetime.datetime
    ) -> None:
        """
        Label the ITF identifications as submitted to the MPC.

        Parameters
        ----------
        submission_id : str
            The submission ID.
        submitted_at : datetime
            The time at which the submission was made.

        Returns
        -------
        None
        """
        # Insure datetime is in UTC and rounded to the nearest millisecond
        submitted_at = submitted_at.astimezone(datetime.timezone.utc)
        submitted_at = round_to_nearest_millisecond(submitted_at)

        # Check current status and raise an error if already submitted
        with self.engine.begin() as conn:

            # Check what the current status is and raise an error if its already been
            # labeled as submitted
            stmt = sq.select(
                self.tables["submissions"].c.itf_identifications_submitted
            ).where(self.tables["submissions"].c.id == submission_id)

            result = conn.execute(stmt).fetchone()[0]

            if result:
                raise ValueError(
                    f"ITF observations for submission {submission_id} have already been marked as submitted."
                )

        # Now, update the submission to mark the ITF identifications as submitted
        with self.engine.begin() as conn:

            stmt = (
                sq.update(self.tables["submissions"])
                .where(self.tables["submissions"].c.id == submission_id)
                .values(
                    itf_identifications_submitted=True,
                    itf_identifications_submitted_at=submitted_at,
                )
            )

            conn.execute(stmt)

            self.logger.info(
                f"ITF identifications for submission {submission_id} marked as submitted."
            )

            stmt = (
                sq.update(self.tables["submission_members"])
                .where(
                    sq.and_(
                        self.tables["submission_members"].c.submission_id
                        == submission_id,
                        self.tables["submission_members"].c.itf_obs_id.isnot(None),
                        self.tables["submission_members"].c.deep_drilling_filtered.is_(
                            False
                        ),
                    )
                )
                .values(submitted=True)
            )

            conn.execute(stmt)

            self.logger.info(
                f"ITF identification members for submission {submission_id} marked as submitted."
            )

        return

    def await_new_observation_ingestion(
        self, submission: Submissions, delay=60, max_attempts=30
    ) -> bool:
        """
        Wait for new observations to be ingested into the MPC database.

        Parameters
        ----------
        submission : Submissions
            The submission table describing the submission
        delay : int, optional
            The delay in seconds between each attempt, by default 60
        max_attempts : int, optional
            The maximum number of attempts, by default 30.

        Returns
        -------
        bool
            True if the observations have been ingested, False otherwise.
        """
        assert len(submission) == 1
        mpc_submission_id = submission.mpc_submission_id.to_pylist()[0]
        new_observations = submission.new_observations.to_pylist()[0]

        counts = 0
        while max_attempts > 0:

            counts = self.client.query_submission_num_obs(mpc_submission_id)

            if counts > 0.9 * new_observations:
                self.logger.info(f"Submission '{mpc_submission_id}' has been ingested.")
                return True
            else:
                self.logger.info("New observations have not been ingested yet.")
                self.logger.info(
                    f"Trying again in {delay} seconds... ({max_attempts} attempts left)."
                )
                time.sleep(delay)
                max_attempts -= 1

        return False
