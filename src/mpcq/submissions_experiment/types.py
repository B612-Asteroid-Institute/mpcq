from dataclasses import dataclass
from typing import Optional

import quivr as qv

from ..qvsql import SQLQuivrTable


class DiscoveryCandidates(qv.Table):
    #: Candidate ID
    trksub = qv.LargeStringColumn()


class DiscoveryCandidateMembers(qv.Table):
    #: Candidate ID
    trksub = qv.LargeStringColumn()
    #: Observation ID
    obssubid = qv.LargeStringColumn()


class AssociationCandidates(qv.Table):
    #: Candidate ID
    trksub = qv.LargeStringColumn()


class AssociationMembers(qv.Table):
    #: Candidate ID
    trksub = qv.LargeStringColumn()
    #: Observation ID
    obssubid = qv.LargeStringColumn()


class Submissions(qv.Table, SQLQuivrTable):
    #: Submission ID
    id = qv.LargeStringColumn()
    #: MPC-assigned submission ID
    mpc_submission_id = qv.LargeStringColumn(nullable=True)
    #: Submitter ID
    submitter_id = qv.Int64Column()
    #: Submission Type: discovery, association, identification
    type = qv.LargeStringColumn()
    #: Number of linkages in the submission
    linkages = qv.Int64Column()
    #: Number of observations in the submission
    observations = qv.Int64Column()
    #: Timestamp when the submission was created
    created_at = qv.TimestampColumn("ms", tz="utc")
    #: Timestamp when the submission was submitted
    submitted_at = qv.TimestampColumn("ms", nullable=True, tz="utc")
    #: Path to the submission file
    file_path = qv.LargeStringColumn()
    #: Hash of the submission file
    file_md5 = qv.LargeStringColumn()
    #: Comment for the submission
    comment = qv.LargeStringColumn(nullable=True)
    #: Error message if the submission failed
    error = qv.LargeStringColumn(nullable=True)


class SubmissionMembers(qv.Table, SQLQuivrTable):
    #: Submission ID
    submission_id = qv.LargeStringColumn()
    #: Observer-assigned permanent ID 
    permid = qv.LargeStringColumn(nullable=True)
    #: Observer-assigned provisional ID
    provid = qv.LargeStringColumn(nullable=True)
    #: trskub of the observation
    trksub = qv.LargeStringColumn()
    #: Observation ID
    obssubid = qv.LargeStringColumn()
    #: MPC-assigned observation ID
    mpc_obsid = qv.LargeStringColumn(nullable=True)
    #: MPC status of the observation
    mpc_status = qv.LargeStringColumn(nullable=True)
    #: MPC-assigned permanent ID
    mpc_permid = qv.LargeStringColumn(nullable=True)
    #: MPC-assigned provisional ID
    mpc_provid = qv.LargeStringColumn(nullable=True)
    #: Last updated (when the observation was last queried for an update)
    updated_at = qv.TimestampColumn("ms", tz="utc", nullable=True)


class Submitters(qv.Table, SQLQuivrTable):
    #: Submitter ID
    id = qv.Int64Column()
    #: First name
    first_name = qv.LargeStringColumn()
    #: Last name
    last_name = qv.LargeStringColumn()
    #: Email
    email = qv.LargeStringColumn()
    #: Institution
    institution = qv.LargeStringColumn(nullable=True)
    #: Timestamp when the submitter was created
    created_at = qv.TimestampColumn("ms", tz="utc")


@dataclass
class Submitter:
    first_name: str
    last_name: str
    email: str
    institution: Optional[str]
    id: Optional[int] = None  # Incremented by the database
