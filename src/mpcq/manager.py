import os
import warnings
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sq

from .client import BigQueryMPCClient, MPCClient
from .submissions import MPCSubmissionResults, SubmissionMembers, Submissions


class SubmissionManager:

    def __init__(
        self, engine: sq.engine.base.Engine, metadata: sq.MetaData, directory: str
    ):
        self.engine = engine
        self.metadata = metadata
        self.tables = metadata.tables
        self.directory = directory

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

        if os.path.exists(os.path.join(directory, "tracking.db")):
            raise FileExistsError("A database already exists in this directory.")

        engine = sq.create_engine("sqlite:///" + os.path.join(directory, "tracking.db"))

        metadata = sq.MetaData()
        sq.Table(
            "submission_members",
            metadata,
            sq.Column("submission_id", sq.Integer),
            sq.Column("orbit_id", sq.String),
            sq.Column("trksub", sq.String),
            sq.Column("obssubid", sq.String),
            sq.Column("deep_drilling_filtered", sq.Boolean),
            sq.Column("itf_obs_id", sq.String, nullable=True),
            sq.Column("submitted", sq.Boolean),
            sq.UniqueConstraint("orbit_id", "obssubid", name="uc_orbit_obssubid"),
        )

        sq.Table(
            "submissions",
            metadata,
            sq.Column("id", sq.Integer, primary_key=True),
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
        submission_ids: Optional[list[int]] = None,
    ) -> MPCSubmissionResults:
        """
        Query the MPC for the results of the submissions.

        Parameters
        ----------
        mpc_submission_ids : list[str]
            The submission_ids to query (submission IDs assigned by the MPC).
        submission_ids : list[int]
            The submission_ids to query (submission IDs assigned by the tracking database).
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

            return self.client.query_submission_results(mpc_submission_ids)

        if mpc_submission_ids is None:
            submissions = self.get_submissions()
            submissions_with_mpc_id = submissions.apply_mask(
                pc.invert(pc.is_null(submissions.mpc_submission_id))
            )
            mpc_submission_ids = submissions_with_mpc_id.mpc_submission_id.to_pylist()

            return self.client.query_submission_results(mpc_submission_ids)
