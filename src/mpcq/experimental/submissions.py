import warnings
from typing import List

import pyarrow.compute as pc
import quivr as qv
from adam_core.time import Timestamp
from astropy.time import Time

from .qvsql import SQLQuivrTable


class Submissions(qv.Table, SQLQuivrTable):
    id = qv.Int64Column()
    mpc_submission_id = qv.LargeStringColumn(nullable=True)
    orbits = qv.Int64Column()
    observations = qv.Int64Column()
    observations_submitted = qv.Int64Column()
    deep_drilling_observations = qv.Int64Column()
    new_observations = qv.Int64Column()
    new_observations_file = qv.LargeStringColumn(nullable=True)
    new_observations_submitted = qv.BooleanColumn()
    new_observations_submitted_at = qv.TimestampColumn("ms", nullable=True, tz="utc")
    itf_observations = qv.Int64Column()
    itf_identifications_file = qv.LargeStringColumn(nullable=True)
    itf_identifications_submitted = qv.BooleanColumn()
    itf_identifications_submitted_at = qv.TimestampColumn("ms", nullable=True, tz="utc")


class SubmissionMembers(qv.Table, SQLQuivrTable):
    orbit_id = qv.LargeStringColumn()
    trksub = qv.LargeStringColumn()
    obssubid = qv.LargeStringColumn()
    submission_id = qv.Int64Column()
    deep_drilling_filtered = qv.BooleanColumn(nullable=True)
    submitted = qv.BooleanColumn(nullable=True)


class TrksubMapping(qv.Table):
    trksub = qv.LargeStringColumn()
    primary_designation = qv.LargeStringColumn(nullable=True)
    permid = qv.LargeStringColumn(nullable=True)
    provid = qv.LargeStringColumn(nullable=True)
    submission_id = qv.LargeStringColumn()
    orbit_id = qv.LargeStringColumn()

    @classmethod
    def from_submissions(
        cls,
        submissions: "Submissions",
        members: "SubmissionMembers",
        results: "MPCSubmissionResults",
    ) -> "TrksubMapping":
        """
        Create a mapping of trksub to primary designation, provid, permid, submission ID for these
        submission members.

        Parameters
        ----------
        submissions : Submissions
            Submissions table.
        members : SubmissionMembers
            Submission members table detailing the observations submitted.
        results : MPCSubmissionResults
            Results of the MPC submission.

        Returns
        -------
        TrksubMapping
            Table of trksub mappings. Each trksub will for each unique primary designation it
            was linked to by the MPC.
        """
        assert pc.all(pc.is_in(results.trksub, members.trksub)).as_py()

        unique_submission_members = members.drop_duplicates(
            ["orbit_id", "trksub", "submission_id"]
        )
        unique_submission_members = unique_submission_members.flattened_table().join(
            submissions.flattened_table().select(["id", "mpc_submission_id"]),
            ("submission_id"),
            ("id"),
        )

        unique_mappings = results.drop_duplicates(
            ["trksub", "primary_designation", "permid", "provid", "submission_id"]
        )

        trksub_mapping = (
            unique_submission_members.join(
                unique_mappings.flattened_table(),
                ("trksub", "mpc_submission_id"),
                ("trksub", "submission_id"),
            )
            .select(
                [
                    "trksub",
                    "primary_designation",
                    "permid",
                    "provid",
                    "submission_id",
                    "orbit_id",
                ]
            )
            .sort_by(
                [
                    ("trksub", "ascending"),
                    ("submission_id", "ascending"),
                    ("primary_designation", "ascending"),
                ]
            )
        )
        return TrksubMapping.from_pyarrow(trksub_mapping)


class MPCSubmissionResults(qv.Table):
    requested_submission_id = qv.LargeStringColumn()
    obsid = qv.LargeStringColumn(nullable=True)
    obssubid = qv.LargeStringColumn(nullable=True)
    trksub = qv.LargeStringColumn(nullable=True)
    primary_designation = qv.LargeStringColumn(nullable=True)
    permid = qv.LargeStringColumn(nullable=True)
    provid = qv.LargeStringColumn(nullable=True)
    submission_id = qv.LargeStringColumn(nullable=True)
    status = qv.LargeStringColumn(nullable=True)


class MPCSubmissionHistory(qv.Table):
    requested_provid = qv.LargeStringColumn()
    primary_designation = qv.LargeStringColumn(nullable=True)
    submission_id = qv.LargeStringColumn(nullable=True)
    submission_time = Timestamp.as_column(nullable=True)
    first_submission = qv.BooleanColumn(nullable=True)
    last_submission = qv.BooleanColumn(nullable=True)
    num_obs = qv.Int64Column(nullable=True)
    first_obs_time = Timestamp.as_column(nullable=True)
    last_obs_time = Timestamp.as_column(nullable=True)
    arc_length = qv.Float64Column(nullable=True)


def infer_submission_time(
    submission_ids: List[str], last_observation_times: Timestamp
) -> Timestamp:
    """
    Infer the submission time from the submission ID and last observation time for
    each submission.

    In some cases, for historical submissions the submission ID is "00000000". In these instances,
    the last observation time is used as the submission time. A warning is raised to alert the user
    that the submission ID is "00000000".

    Parameters
    ----------
    submission_ids : list of str
        List of submission IDs.
    last_observation_times : Timestamp
        Last observation time for each submission.

    Returns
    -------
    Timestamp
        Submission time for each submission.
    """
    times_isot = []
    for i, (submission_id, last_observation_time) in enumerate(
        zip(submission_ids, last_observation_times)
    ):
        if submission_id == "00000000":
            submission_time = last_observation_time
            warnings.warn(
                f"Submission ID is 00000000 for observation at index {i}. Using observation time as submission time."
            )
        else:
            submission_time = submission_id.split("_")[0]

        times_isot.append(submission_time)

    return Timestamp.from_astropy(Time(times_isot, format="isot", scale="utc"))
