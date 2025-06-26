import logging
import os
import queue as qu
import sys
import time
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
from .mpc import MPCSubmissionClient
from .types import (
    AssociationCandidates,
    AssociationMembers,
    DiscoveryCandidateMembers,
    DiscoveryCandidates,
    SubmissionMembers,
    Submissions,
    Submitter,
    Submitters,
)
from .utils import (
    candidates_to_ades,
    compute_file_md5,
    generate_submission_id,
    round_to_nearest_millisecond,
)

DEFAULT_ADES_CONFIG = {
    "seconds_precision": 3,
    "columns_precision": {
        "ra": 9,
        "dec": 9,
        "rmsRACosDec": 5,
        "rmsDec": 5,
        "rmsCorr": 8,
        "mag": 4,
        "rmsMag": 4,
        "exp": 2,
        "logSNR": 2,
        "seeing": 2,
    },
}


class SubmissionManager:

    def __init__(
        self, engine: sq.engine.base.Engine, metadata: sq.MetaData, directory: str
    ):
        self.engine = engine
        self.metadata = metadata
        self.tables = metadata.tables
        self.directory = directory
        self.submission_directory = os.path.join(directory, "submissions")
        self._submitter = None
        self._mpc_submission_client = None
        self._queue = qu.Queue()
        self.setup_logging()

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

    @property
    def submitter(self) -> Submitter:
        """
        The submitter details.
        """
        return self._submitter

    @submitter.deleter
    def submitter(self) -> None:
        """
        Delete the submitter details.
        """
        self._submitter = None
        self.logger.info("Submitter deleted.")

    @property
    def mpc_submission_client(self) -> MPCSubmissionClient:
        """
        The MPC submission client.
        """
        return self._mpc_submission_client

    @mpc_submission_client.setter
    def mpc_submission_client(self, value: MPCSubmissionClient) -> None:
        """
        Set the MPC submission client.
        """
        self._mpc_submission_client = value
        self.logger.info(f"MPC submission client set to {value.__class__.__name__}.")

    @mpc_submission_client.deleter
    def mpc_submission_client(self) -> None:
        """
        Delete the MPC submission client.
        """
        self._mpc_submission_client = None
        self.logger.info("MPC submission client deleted.")

    def select_submitter(self) -> None:
        """
        Select the submitter details.
        """
        # Get all submitters from database
        with self.engine.begin() as conn:
            statement = sq.select(self.tables["submitters"])
            result = conn.execute(statement)
            submitters = result.fetchall()

        if len(submitters) > 0:
            print("\nAvailable submitters:")
            for i, submitter in enumerate(submitters):
                print(
                    f"{i+1}. {submitter.first_name} {submitter.last_name} ({submitter.email})"
                )
            print("\n0. Add new submitter")

            while True:
                try:
                    choice = int(
                        input(
                            "\nSelect submitter (0-{num}): ".format(num=len(submitters))
                        )
                    )
                    if 0 <= choice <= len(submitters):
                        break
                    print("Invalid choice. Please try again.")
                except ValueError:
                    print("Invalid input. Please enter a number.")

            if choice == 0:
                self._prompt_new_submitter()
            else:
                selected = submitters[choice - 1]
                self._submitter = Submitter(
                    first_name=selected.first_name,
                    last_name=selected.last_name,
                    email=selected.email,
                    institution=selected.institution if selected.institution else None,
                    id=selected.id,
                )
        else:
            print("No submitters found in database.")
            self._prompt_new_submitter()

    def _prompt_new_submitter(self) -> None:
        """Helper method to add a new submitter to the database."""

        while True:
            first_name = input("Enter first name: ")
            last_name = input("Enter last name: ")
            email = input("Enter email: ")
            institution = input("Enter institution (optional): ") or None

            # Create new submitter
            new_submitter = Submitter(
                first_name=first_name,
                last_name=last_name,
                email=email,
                institution=institution,
            )

            # Confirm if
            print(
                f"New submitter:\n first_name: {new_submitter.first_name}\n last_name: {new_submitter.last_name}\n email: {new_submitter.email}\n institution: {new_submitter.institution}"
            )
            confirm = input("Is this correct? (y/n): ")
            if confirm == "y":
                break
            else:
                print("Please try again.")

        with self.engine.begin() as conn:
            statement = (
                self.tables["submitters"]
                .insert()
                .values(
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    institution=institution if institution else None,
                    created_at=round_to_nearest_millisecond(datetime.now(timezone.utc)),
                )
            )
            conn.execute(statement)
            conn.commit()

        with self.engine.begin() as conn:
            submitter_id = conn.execute(
                sq.select(self.tables["submitters"].c.id).order_by(
                    self.tables["submitters"].c.id.desc()
                )
            ).fetchone()[0]

        self._submitter = Submitter(
            first_name=first_name,
            last_name=last_name,
            email=email,
            institution=institution if institution else None,
            id=submitter_id,
        )
        self.logger.info(
            f"New submitter added: {self.submitter.first_name} {self.submitter.last_name}, {self.submitter.email}, {self.submitter.institution}."
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
            sq.Column("submitter_id", sq.Integer),
            sq.Column("type", sq.String),
            sq.Column("linkages", sq.Integer),
            sq.Column("observations", sq.Integer),
            sq.Column("created_at", sq.DateTime),
            sq.Column("submitted_at", sq.DateTime, nullable=True),
            sq.Column("file_path", sq.String),
            sq.Column("file_md5", sq.String),
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

        sq.Table(
            "submitters",
            metadata,
            sq.Column("id", sq.Integer, primary_key=True, autoincrement=True),
            sq.Column("first_name", sq.String),
            sq.Column("last_name", sq.String),
            sq.Column("email", sq.String),
            sq.Column("institution", sq.String, nullable=True),
            sq.Column("created_at", sq.DateTime),
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

    def get_submitters(self, submitter_ids: Optional[List[int]] = None) -> Submitters:
        """
        Get the submitters from the SubmissionManager tracking database.

        Parameters
        ----------
        submitter_ids : Optional[List[int]], optional
            The IDs of the submitters to get, by default None.

        Returns
        -------
        Submitters
            The submitters.
        """
        if submitter_ids is not None:
            statement = sq.select(self.tables["submitters"]).where(
                self.tables["submitters"].c.id.in_(submitter_ids)
            )
        else:
            statement = sq.select(self.tables["submitters"])

        return Submitters.from_sql(self.engine, "submitters", statement=statement)

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
        max_observations_per_file: Optional[int] = 50000,
        discovery_comment: Optional[str] = None,
        association_comment: Optional[str] = None,
        columns_precision: Optional[Dict[str, int]] = None,
        seconds_precision: Optional[int] = None,
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
        columns_precision : Optional[Dict[str, int]], optional
            The precision for the columns in the ADES files.
        seconds_precision : Optional[int], optional
            The precision for the seconds in the ADES files.

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

        if self._submitter is None:
            self.select_submitter()
        submitter_id = self._submitter.id

        if columns_precision is None:
            columns_precision = DEFAULT_ADES_CONFIG["columns_precision"]
        if seconds_precision is None:
            seconds_precision = DEFAULT_ADES_CONFIG["seconds_precision"]

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
                    f"Preparing discovery ADES file {i + 1} ('{submission_id}')"
                )

                file_path = os.path.join(base_dir, f"{submission_id}.psv")

                self.logger.info(f"Saving discovery ADES to {file_path}")
                with open(file_path, "w") as f:
                    f.write(
                        ADES_to_string(
                            discovery_ades_i,
                            obscontexts,
                            columns_precision=columns_precision,
                            seconds_precision=seconds_precision,
                        )
                    )

                # Create a new submission
                submission_i = Submissions.from_kwargs(
                    id=[submission_id],
                    mpc_submission_id=None,
                    type=["discovery"],
                    linkages=[len(discovery_ades_i.trkSub.unique())],
                    observations=[len(discovery_ades_i)],
                    created_at=[datetime.now().astimezone(timezone.utc)],
                    submitted_at=None,
                    file_path=[file_path],
                    file_md5=[compute_file_md5(file_path)],
                    comment=[discovery_comment],
                    submitter_id=[submitter_id],
                )

                submission_members_i = SubmissionMembers.from_kwargs(
                    submission_id=pa.repeat(submission_id, len(discovery_ades_i)),
                    trksub=discovery_ades_i.trkSub,
                    obssubid=discovery_ades_i.obsSubID,
                )

                try:
                    # Save submissions and submission members to the database
                    submission_members_i.to_sql(
                        self.engine, "submission_members", if_exists="fail"
                    )
                    submission_i.to_sql(self.engine, "submissions", if_exists="fail")
                    self.logger.debug(
                        f"Saved discovery submission {submission_id} to database"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error saving discovery submission {submission_id} to database: {e}"
                    )
                    os.remove(file_path)
                    raise e

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
                    f"Preparing association ADES file {i + 1} ('{submission_id}')"
                )

                file_path = os.path.join(base_dir, f"{submission_id}.psv")

                self.logger.info(f"Saving association ADES to {file_path}")
                with open(file_path, "w") as f:
                    f.write(
                        ADES_to_string(
                            association_ades_i,
                            obscontexts,
                            columns_precision=columns_precision,
                            seconds_precision=seconds_precision,
                        )
                    )

                # Create a new submission
                submission_i = Submissions.from_kwargs(
                    id=[submission_id],
                    mpc_submission_id=None,
                    type=["association"],
                    linkages=[len(association_ades_i.trkSub.unique())],
                    observations=[len(association_ades_i)],
                    created_at=[datetime.now().astimezone(timezone.utc)],
                    submitted_at=None,
                    file_path=[file_path],
                    file_md5=[compute_file_md5(file_path)],
                    comment=[association_comment],
                    submitter_id=[submitter_id],
                )

                submission_members_i = SubmissionMembers.from_kwargs(
                    submission_id=pa.repeat(submission_id, len(association_ades_i)),
                    trksub=association_ades_i.trkSub,
                    obssubid=association_ades_i.obsSubID,
                )

                try:
                    # Save submissions and submission members to the database
                    submission_members_i.to_sql(
                        self.engine, "submission_members", if_exists="fail"
                    )
                    submission_i.to_sql(self.engine, "submissions", if_exists="fail")
                    self.logger.debug(
                        f"Saved association submission {submission_id} to database"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error saving association submission {submission_id} to database: {e}"
                    )
                    os.remove(file_path)
                    raise e

                submissions = qv.concatenate([submissions, submission_i])
                submission_members = qv.concatenate(
                    [submission_members, submission_members_i]
                )

    def delete_prepared_submissions(
        self, submission_ids: Optional[List[str]] = None
    ) -> None:
        """
        Delete any submissions (and their files) that have not been submitted.

        Returns
        -------
        None
        """
        submissions = self.get_submissions(submission_ids=submission_ids)
        submissions_to_clear = submissions.apply_mask(
            pc.is_null(submissions.submitted_at)
        )

        if len(submissions_to_clear) == 0:
            self.logger.info("No submissions to clear")
            return

        for submission in submissions_to_clear:
            submission_file = submission.file_path[0].as_py()
            submission_file_md5 = submission.file_md5[0].as_py()
            if submission_file_md5 != compute_file_md5(submission_file):
                self.logger.warning(
                    f"Submission file {submission_file} has been modified since it was prepared"
                )
                continue

            if os.path.exists(submission_file):
                os.remove(submission_file)
            self.logger.debug(f"Removed submission file {submission_file}")

        with self.engine.begin() as conn:
            stmt = sq.delete(self.tables["submissions"]).where(
                self.tables["submissions"].c.id.in_(submissions_to_clear.id.to_pylist())
            )
            conn.execute(stmt)
            self.logger.info(
                f"Cleared {len(submissions_to_clear)} submissions from the database"
            )

            stmt = sq.delete(self.tables["submission_members"]).where(
                self.tables["submission_members"].c.submission_id.in_(
                    submissions_to_clear.id.to_pylist()
                )
            )
            conn.execute(stmt)
            self.logger.info(
                f"Cleared {len(submissions_to_clear)} submission members from the database"
            )

        return

    def queue_for_submission(
        self,
        submission_ids: List[str],
    ):
        """
        Queue the given submissions for submission to the MPC. This function will populate
        the queue with the submission ID and the submission file path.

        Parameters
        ----------
        submission_ids : List[str]
            The IDs of the submissions to queue for submission.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If no submissions are found for the given IDs.
            If any of the submissions have already been submitted.
        """
        submissions = self.get_submissions(submission_ids=submission_ids)
        if len(submissions) == 0:
            raise ValueError(f"No submissions found for IDs: {submission_ids}")

        already_submitted = submissions.apply_mask(
            pc.invert(pc.is_null(submissions.submitted_at))
        )
        if len(already_submitted) > 0:
            raise ValueError(
                f"Submissions {already_submitted.submission_id.as_pylist()} have already been submitted."
            )

        for submission in submissions:
            submission_id = submission.id[0].as_py()
            submission_file = submission.file_path[0].as_py()

            assert os.path.exists(
                submission_file
            ), f"Submission file {submission_file} does not exist"

            self.logger.info(f"Queuing submission '{submission_id}' for submission")
            self._queue.put((submission_id, submission_file))

    def submit_from_queue(self) -> None:
        """
        Submit the next submission in the queue.

        Returns
        -------
        None
        """
        if self.submitter is None:
            raise ValueError("Submitter not set")

        if self.queue.qsize() == 0:
            self.logger.info("No submissions in the queue")
            return

        submission_id, submission_file = self.queue.get()

        self.logger.info(f"Retrieved submission '{submission_id}' from the queue")

        submission = self.get_submissions(submission_ids=[submission_id])
        submission_members = self.get_submission_members(submission_ids=[submission_id])
        if len(submission) == 0:
            self.logger.error(f"Submission {submission_id} not found in the database")
            raise ValueError(f"Submission {submission_id} not found in the database")

        if len(submission_members) == 0:
            self.logger.error(
                f"Submission members '{submission_id}' not found in the database"
            )

            raise ValueError(
                f"Submission members '{submission_id}' not found in the database"
            )

        submission_type = submission.type[0].as_py()
        submission_file_path = submission.file_path[0].as_py()
        submission_file_md5 = submission.file_md5[0].as_py()
        submission_comment = submission.comment[0].as_py()
        submitter = self.get_submitters(
            submitter_ids=[submission.submitter_id[0].as_py()]
        )
        submitter_email = submitter.email[0].as_py()

        if submission_type == "discovery" or submission_type == "association":

            try:
                mpc_submission_id, submission_time = (
                    self.mpc_submission_client.submit_ades(
                        submission_file_path,
                        submitter_email,
                        submission_comment + f" ({submission_file_md5})",
                    )
                )
                self._set_submission_success(
                    submission_id, mpc_submission_id, submission_time
                )

            except Exception as e:
                self.logger.error(f"Error submitting submission '{submission_id}': {e}")
                self._set_submission_failure(submission_id, error=e)
                return

        elif submission_type == "identification":
            raise NotImplementedError("Identifications are not implemented yet")
        else:
            raise ValueError(f"Invalid submission type: {submission_type}")

        self.logger.info(f"Submission '{submission_id}' submitted successfully")

    def submit_queue(self, delay: Optional[timedelta] = timedelta(seconds=10)) -> None:
        """
        Submit the queue of submissions to the MPC.

        Parameters
        ----------
        delay : Optional[timedelta], optional
            The delay between submissions, by default 10 seconds.

        Returns
        -------
        None
        """
        while not self._queue.empty():
            self.submit_from_queue()
            time.sleep(delay.total_seconds())

    def _set_submission_success(
        self,
        submission_id: str,
        mpc_submission_id: str,
        submitted_at: datetime,
    ):
        # Insure datetime is in UTC and rounded to the nearest millisecond
        submitted_at = submitted_at.astimezone(timezone.utc)
        submitted_at = round_to_nearest_millisecond(submitted_at)

        # Check current status and raise an error if already submitted
        with self.engine.begin() as conn:

            stmt = sq.select(self.tables["submissions"].c.submitted_at).where(
                self.tables["submissions"].c.id == submission_id
            )

            result = conn.execute(stmt).fetchone()[0]

            if result:
                raise ValueError(
                    f"Submission '{submission_id}' has already been marked as submitted."
                )

        # Now, update the submission to mark it as submitted
        with self.engine.begin() as conn:

            stmt = (
                sq.update(self.tables["submissions"])
                .where(self.tables["submissions"].c.id == submission_id)
                .values(submitted_at=submitted_at, mpc_submission_id=mpc_submission_id)
            )

            conn.execute(stmt)

            self.logger.info(
                f"Submission '{submission_id}' marked as submitted (MPC submission ID: '{mpc_submission_id}')."
            )

        return

    def _set_submission_failure(
        self,
        submission_id: str,
        error: Exception,
        mpc_submission_id: Optional[str] = None,
    ):
        with self.engine.begin() as conn:

            stmt = (
                sq.update(self.tables["submissions"])
                .where(self.tables["submissions"].c.id == submission_id)
                .values(
                    submitted_at=None,
                    mpc_submission_id=mpc_submission_id,
                    error=str(error),
                )
            )

            conn.execute(stmt)
            self.logger.error(f"Submission '{submission_id}' failed to submit: {error}")

        return
