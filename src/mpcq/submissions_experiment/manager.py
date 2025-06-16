import logging
import os
import queue as qu
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import quivr as qv
import sqlalchemy as sq
from adam_core.observations import SourceCatalog
from adam_core.observations.ades import ADES_to_string, ADESObservations, ObsContext
from tqdm import tqdm

from ..client import BigQueryMPCClient, MPCClient
from .types import (
    AssociationCandidates,
    AssociationMembers,
    DiscoveryCandidateMembers,
    DiscoveryCandidates,
    SubmissionMembers,
    Submissions,
    Submitter,
)
from .utils import candidates_to_ades, generate_submission_id


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
        self.load_queue()

    @property
    def queue(self) -> qu.Queue:
        """
        The queue of submissions to submit to the MPC.
        """
        return self._queue

    @queue.setter
    def queue(self, value: qu.Queue) -> None:
        """
        Set the queue of submissions to submit to the MPC.
        """
        raise NotImplementedError(
            "The queue is read-only. To reload the queue, run self.reload_queue()"
        )

    @queue.deleter
    def queue(self) -> None:
        """
        Delete the queue of submissions to submit to the MPC.
        """
        raise NotImplementedError(
            "The queue is read-only. To clear the queue, run self.clear_queue()"
        )

    def load_queue(self) -> None:
        """
        Load the last day's queue of submissions to submit to the MPC.
        """
        self.clear_queue()

        # Read submissions from the database
        submissions = self.get_submissions(
            since=datetime.now(timezone.utc) - timedelta(days=1)
        )

        # Filter for those that are unsubmitted
        submissions = submissions.apply_mask(pc.is_null(submissions.mpc_submission_id))
        self.logger.info(f"Found {len(submissions)} unsubmitted submissions to queue")
        self.queue_for_submission(submissions)

    def clear_queue(self) -> None:
        """
        Clear the queue of submissions to submit to the MPC.
        """
        self._queue = qu.Queue()

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
            sq.Column("trksub", sq.String),
            sq.Column("obssubid", sq.String, primary_key=True),
            sq.Column("mpc_obsid", sq.String, nullable=True),
            sq.Column("mpc_status", sq.String, nullable=True),
            sq.Column("mpc_permid", sq.String, nullable=True),
            sq.Column("mpc_provid", sq.String, nullable=True),
            sq.Column("updated_at", sq.DateTime),
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

    def get_submissions(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        submission_ids: Optional[List[str]] = None,
    ) -> Submissions:
        """
        Get submissions from the SubmissionManager tracking database.

        Parameters
        ----------
        since : Optional[datetime], optional
            The date and time to get submissions from, by default None.
        until : Optional[datetime], optional
            The date and time to get submissions until, by default None.
        submission_ids : Optional[List[str]], optional
            The IDs of the submissions to get, by default None.

        Returns
        -------
        Submissions
            The submissions.
        """
        statement = sq.select(self.tables["submissions"])
        if since is not None:
            statement = statement.where(
                self.tables["submissions"].c.created_at >= since
            )
        if until is not None:
            statement = statement.where(
                self.tables["submissions"].c.created_at <= until
            )
        if submission_ids is not None:
            statement = statement.where(
                self.tables["submissions"].c.id.in_(submission_ids)
            )

        return Submissions.from_sql(self.engine, "submissions", statement=statement)

    def get_submission_members(
        self, submission_ids: List[str] = None
    ) -> SubmissionMembers:
        """
        Get the members of a submission from the SubmissionManager tracking database.

        Parameters
        ----------
        submission_ids : List[str]
            The IDs of the submissions to get the members of.

        Returns
        -------
        SubmissionMembers
            The members of the submissions.
        """
        statement = sq.select(self.tables["submission_members"])
        if submission_ids is not None:
            statement = statement.where(
                self.tables["submission_members"].c.submission_id.in_(submission_ids)
            )

        return SubmissionMembers.from_sql(
            self.engine, "submission_members", statement=statement
        )

    def prepare_submissions(
        self,
        source_catalog: SourceCatalog,
        obscontexts: Dict[str, ObsContext],
        discovery_candidates: Optional[DiscoveryCandidates] = None,
        discovery_candidate_members: Optional[DiscoveryCandidateMembers] = None,
        association_candidates: Optional[AssociationCandidates] = None,
        association_members: Optional[AssociationMembers] = None,
        max_observations_per_file: Optional[int] = 10000,
        discovery_comment: Optional[str] = None,
        association_comment: Optional[str] = None,
    ) -> Tuple[Submissions, SubmissionMembers]:
        """
        Generate ADES PSV files for the given candidates and their members. The ADES PSV files are saved to the submission directory
        in a folder with the current date as the prefix.

        Discovery candidate submissions will have the type "discovery" and association candidate submissions will have the type "association".

        Parameters
        ----------
        source_catalog : SourceCatalog
            The source catalog.
        obscontexts : Dict[str, ObsContext]
            The ObsContexts to use for the ADES header data.
        discovery_candidates : Optional[DiscoveryCandidates], optional
            The discovery candidates. These are objects believed to be newly observed.
        discovery_candidate_members : Optional[DiscoveryCandidateMembers], optional
            The observation members of the discovery candidates.
        association_candidates : Optional[AssociationCandidates], optional
            The association candidates. These are objects believed to be associated with a known object.
        association_members : Optional[AssociationMembers], optional
            The observation members of the association candidates.
        max_observations_per_file : Optional[int], optional
            The maximum number of observations per file, by default 100000.
        discovery_comment : Optional[str], optional
            The comment for the discovery submissions, by default None.
        association_comment : Optional[str], optional
            The comment for the association submissions, by default None.

        Returns
        -------
        discovery_ades, association_ades : tuple[List[ADESObservations], List[ADESObservations]]
            The ADESObservations for the discovery and association candidates.
        """
        if discovery_candidates is None and association_candidates is None:
            raise ValueError(
                "At least one of discovery_candidates or association_candidates must be provided."
            )

        if discovery_candidates is not None:
            if discovery_candidate_members is None:
                raise ValueError(
                    "discovery_candidate_members must be provided if discovery_candidates is provided."
                )

            if not pc.all(
                pc.is_in(
                    discovery_candidates.trksub, discovery_candidate_members.trksub
                )
            ).as_py():
                raise ValueError(
                    "All trksubs in discovery_candidates must be present in discovery_candidate_members."
                )

            if not pc.all(
                pc.is_in(
                    discovery_candidate_members.trksub, discovery_candidates.trksub
                )
            ).as_py():
                raise ValueError(
                    "All trksubs in discovery_candidate_members must be present in discovery_candidates."
                )

            if not pc.all(
                pc.is_in(discovery_candidate_members.obssubid, source_catalog.id)
            ).as_py():
                raise ValueError(
                    "All obssubids in discovery_candidate_members must be present in source_catalog."
                )

        if association_candidates is not None:
            if association_members is None:
                raise ValueError(
                    "association_members must be provided if association_candidates is provided."
                )

            if not pc.all(
                pc.is_in(association_candidates.trksub, association_members.trksub)
            ).as_py():
                raise ValueError(
                    "All trksubs in association_candidates must be present in association_members."
                )

            if not pc.all(
                pc.is_in(association_members.trksub, association_candidates.trksub)
            ).as_py():
                raise ValueError(
                    "All trksubs in association_members must be present in association_candidates."
                )

            if not pc.all(
                pc.is_in(association_members.obssubid, source_catalog.id)
            ).as_py():
                raise ValueError(
                    "All obssubids in association_members must be present in source_catalog."
                )

        submissions = Submissions.empty()
        submission_members = SubmissionMembers.empty()

        submission_id_prefix = datetime.now().strftime("%Y%m%d")
        base_dir = os.path.join(self.submission_directory, submission_id_prefix)
        os.makedirs(base_dir, exist_ok=True)

        # Create ADES Observations for discovery candidates
        if discovery_candidates is not None:
            if len(discovery_candidates) == 0:
                discovery_ades = [ADESObservations.empty()]
            else:
                discovery_ades = candidates_to_ades(
                    discovery_candidates,
                    discovery_candidate_members,
                    source_catalog,
                    max_observations_per_table=max_observations_per_file,
                )

            self.logger.info(
                f"Processing {len(discovery_ades)} discovery ADES files for submission {submission_id_prefix}"
            )
            for i, discovery_ades_i in enumerate(
                tqdm(discovery_ades, desc="Processing discovery ADES files")
            ):

                submission_id = generate_submission_id(
                    "discovery", prefix=submission_id_prefix
                )

                self.logger.info(
                    f"Preparing discovery ADES file {i + 1} ({submission_id})"
                )

                file_path = os.path.join(base_dir, f"{submission_id}.psv")

                # Create a new submission
                submission_i = Submissions.from_kwargs(
                    id=[submission_id],
                    mpc_submission_id=None,
                    type=["discovery"],
                    linkages=[len(discovery_ades_i.trkSub.unique())],
                    observations=[len(discovery_ades_i)],
                    created_at=[datetime.now().astimezone(timezone.utc)],
                    submitted_at=None,
                    file=[file_path],
                    comment=[discovery_comment],
                )

                submission_members_i = SubmissionMembers.from_kwargs(
                    submission_id=pa.repeat(submission_id, len(discovery_ades_i)),
                    trksub=discovery_ades_i.trkSub,
                    obssubid=discovery_ades_i.obsSubID,
                )

                self.logger.info(f"Saving discovery ADES to {file_path}")
                with open(file_path, "w") as f:
                    f.write(ADES_to_string(discovery_ades_i, obscontexts))

                # Save submissions and submission members to the database
                submission_i.to_sql(self.engine, "submissions")
                submission_members_i.to_sql(self.engine, "submission_members")
                self.logger.debug(
                    f"Saved discovery submission {submission_id} to database"
                )

                submissions = qv.concatenate([submissions, submission_i])
                submission_members = qv.concatenate(
                    [submission_members, submission_members_i]
                )

        # Create ADES Observations for association candidates
        if association_candidates is not None:
            if len(association_candidates) == 0:
                association_ades = [ADESObservations.empty()]
            else:
                association_ades = candidates_to_ades(
                    association_candidates,
                    association_members,
                    source_catalog,
                    max_observations_per_table=max_observations_per_file,
                )

            self.logger.info(
                f"Processing {len(association_ades)} association ADES files for submission {submission_id_prefix}"
            )
            for i, association_ades_i in enumerate(
                tqdm(association_ades, desc="Processing association ADES files")
            ):

                submission_id = generate_submission_id(
                    "association", prefix=submission_id_prefix
                )

                self.logger.info(
                    f"Preparing association ADES file {i + 1} ({submission_id})"
                )

                file_path = os.path.join(base_dir, f"{submission_id}.psv")

                # Create a new submission
                submission_i = Submissions.from_kwargs(
                    id=[submission_id],
                    mpc_submission_id=None,
                    type=["association"],
                    linkages=[len(association_ades_i.trkSub.unique())],
                    observations=[len(association_ades_i)],
                    created_at=[datetime.now().astimezone(timezone.utc)],
                    submitted_at=None,
                    file=[file_path],
                    comment=[association_comment],
                )

                submission_members_i = SubmissionMembers.from_kwargs(
                    submission_id=pa.repeat(submission_id, len(association_ades_i)),
                    trksub=association_ades_i.trkSub,
                    obssubid=association_ades_i.obsSubID,
                )

                self.logger.info(f"Saving association ADES to {file_path}")
                with open(file_path, "w") as f:
                    f.write(ADES_to_string(association_ades_i, obscontexts))

                # Save submissions and submission members to the database
                submission_i.to_sql(self.engine, "submissions")
                submission_members_i.to_sql(self.engine, "submission_members")
                self.logger.debug(
                    f"Saved association submission {submission_id} to database"
                )

                submissions = qv.concatenate([submissions, submission_i])
                submission_members = qv.concatenate(
                    [submission_members, submission_members_i]
                )

        return submissions, submission_members

    def queue_for_submission(
        self,
        submissions: Submissions,
    ) -> Tuple[Submissions, SubmissionMembers]:
        """
        Queue the given submissions for submission to the MPC. This function will populate
        the queue with the submission ID and the submission file path.

        Parameters
        ----------
        submissions : Submissions
            The submissions to queue for submission to the MPC.

        Returns
        -------
        submissions : Submissions
            The submissions.
        """
        for submission in submissions:
            submission_id = submission.id[0].as_py()
            submission_file = submission.file[0].as_py()

            assert os.path.exists(
                submission_file
            ), f"Submission file {submission_file} does not exist"

            self.logger.info(f"Queuing submission {submission_id} for submission")
            self._queue.put((submission_id, submission_file))
