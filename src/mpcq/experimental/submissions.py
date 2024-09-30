import warnings
from typing import List

import pyarrow.compute as pc
import quivr as qv
from adam_core.time import Timestamp
from astropy.time import Time


class SubmissionDetails(qv.Table):
    orbit_id = qv.LargeStringColumn()
    trksub = qv.LargeStringColumn()
    obssubid = qv.LargeStringColumn()
    submission_id = qv.LargeStringColumn()

    def trksub_mapping(
        self, mpc_submission_info: "MPCSubmissionInfo"
    ) -> "TrksubMapping":
        """
        Create a mapping of trksub to primary designation, provid, permid, submission ID for these
        submission details.

        Parameters
        ----------
        mpc_submission_info : MPCSubmissionInfo
            Table of submission results from the MPC. See `MPCClient.query_submission_info`.

        Returns
        -------
        TrksubMapping
            Table of trksub mappings. Each trksub will for each unique primary designation it
            was linked to by the MPC.
        """
        assert pc.all(pc.is_in(mpc_submission_info.trksub, self.trksub)).as_py()

        unique_submission_details = self.drop_duplicates(
            ["orbit_id", "trksub", "submission_id"]
        )

        unique_mappings = self.drop_duplicates(
            ["trksub", "primary_designation", "provid", "permid", "submission_id"]
        )

        trksub_mapping = (
            unique_submission_details.table.join(
                unique_mappings.table,
                ("trksub", "submission_id"),
                ("trksub", "submission_id"),
            )
            .select(
                [
                    "trksub",
                    "primary_designation",
                    "provid",
                    "permid",
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


class TrksubMapping(qv.Table):

    trksub = qv.LargeStringColumn()
    primary_designation = qv.LargeStringColumn(nullable=True)
    provid = qv.LargeStringColumn(nullable=True)
    permid = qv.LargeStringColumn(nullable=True)
    submission_id = qv.LargeStringColumn()
    orbit_id = qv.LargeStringColumn()


class MPCSubmissionInfo(qv.Table):
    obsid = qv.LargeStringColumn()
    obssubid = qv.LargeStringColumn(nullable=True)
    primary_designation = qv.LargeStringColumn(nullable=True)
    trksub = qv.LargeStringColumn()
    provid = qv.LargeStringColumn(nullable=True)
    permid = qv.LargeStringColumn(nullable=True)
    submission_id = qv.LargeStringColumn()
    status = qv.LargeStringColumn(nullable=True)


class MPCSubmissionHistory(qv.Table):
    primary_designation = qv.LargeStringColumn()
    submission_id = qv.LargeStringColumn()
    submission_time = Timestamp.as_column()
    first_submission = qv.BooleanColumn()
    last_submission = qv.BooleanColumn()
    num_obs = qv.Int64Column()
    first_obs_time = Timestamp.as_column()
    last_obs_time = Timestamp.as_column()
    arc_length = qv.Float64Column()


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
