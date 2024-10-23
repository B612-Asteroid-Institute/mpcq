import logging
import os
import warnings
from dataclasses import dataclass
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
import quivr as qv
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
        self.logger = logging.getLogger("SubmissionManager")

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
        engine = sq.create_engine("sqlite:///" + os.path.join(directory, "tracking.db"))

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
