import logging
import os
import queue
import sys
from typing import Optional

import sqlalchemy as sq

from ..client import BigQueryMPCClient, MPCClient
from .types import Submitter


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
        self.queue = queue.Queue()
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
            "submissions",
            metadata,
            sq.Column("id", sq.String, primary_key=True),
            sq.Column("mpc_submission_id", sq.String, nullable=True),
            sq.Column("type", sq.String),
            sq.Column("linkages", sq.Integer),
            sq.Column("observations", sq.Integer),
            sq.Column("created_at", sq.DateTime),
            sq.Column("submitted_at", sq.DateTime, nullable=True),
            sq.Column("file", sq.String),
            sq.Column("comment", sq.String, nullable=True),
            sq.Column("error", sq.String, nullable=True),
        )

        sq.Table(
            "submission_members",
            metadata,
            sq.Column("submission_id", sq.String),
            sq.Column("mpc_obs_id", sq.String, nullable=True),
            sq.Column("mpc_status", sq.String, nullable=True),
            sq.Column("trksub", sq.String),
            sq.Column("obssubid", sq.String, primary_key=True),
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
