import warnings
from typing import List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import quivr as qv
from adam_core.observations import ADESObservations, SourceCatalog
from adam_core.orbit_determination import FittedOrbitMembers, FittedOrbits
from adam_core.time import Timestamp
from astropy.time import Time

from .identifications import Identifications
from .qvsql import SQLQuivrTable
from .utils import orbit_id_to_trksub, reduce_deep_drilling_observations


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
    submission_id = qv.Int64Column()
    orbit_id = qv.LargeStringColumn()
    trksub = qv.LargeStringColumn()
    obssubid = qv.LargeStringColumn()
    deep_drilling_filtered = qv.BooleanColumn(nullable=True)
    itf_obs_id = qv.LargeStringColumn(nullable=True)
    submitted = qv.BooleanColumn(default=False)


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


class MPCCrossmatch(qv.Table):
    obs_id = qv.LargeStringColumn()
    mpc_id = qv.LargeStringColumn()
    time_difference = qv.Float64Column()
    distance = qv.Float64Column()
    status = qv.LargeStringColumn(nullable=True)
    trksub = qv.LargeStringColumn(nullable=True)


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


def prepare_submission(
    submission_id: int,
    orbits: FittedOrbits,
    orbit_members: FittedOrbitMembers,
    observations: SourceCatalog,
    max_obs_per_night: Optional[int] = 6,
    mpc_crossmatch: Optional[MPCCrossmatch] = None,
    astrometric_catalog: Optional[str] = "Gaia2",
) -> tuple[SubmissionMembers, ADESObservations, Identifications]:
    """
    Prepare a new submission for the MPC.

    Generates an ADESObservations table for any new observations that will be submitted. If any orbits
    have more than `max_obs_per_night` observations in a single night, these observations will be reduced to `max_obs_per_night`
    observations per night. The observations are selected in such away to maximize the time sampling of orbit as observed within the
    night. If an MPC crossmatch is provided, the crossmatch will be used to identify any potential orbit member observations that are
    within 2 seconds and 2 arcseconds of an MPC observation. These observations will be submitted as "ITF-ITF" identifications.

    Parameters
    ----------
    submission_id : int
        The submission ID.
    orbits : FittedOrbits
        The fitted orbits.
    orbit_members : FittedOrbitMembers
        The fitted orbit members.
    observations : SourceCatalog
        The source catalog containing the observation details for all orbit members.
    max_obs_per_night : Optional[int], optional
        The maximum number of observations per night, by default 6.
    mpc_crossmatch : Optional[MPCCrossmatch], optional
        The MPC crossmatch, by default None.
    astrometric_catalog : Optional[str], optional
        The astrometric catalog to use for the observations, by default "Gaia2".

    Returns
    -------
    SubmissionMembers
        The submission members table detailing which observations were identified as
        new, ITF crossmatches, and deep drilling filtered.
    ADESObservations
        The new observations that will be submitted.
    Identifications
        The identifications table detailing the ITF-ITF linkages.
    """
    # Filter orbit_members to only include those orbits that are in the submission
    orbit_members = orbit_members.apply_mask(
        pc.is_in(orbit_members.orbit_id, orbits.orbit_id)
    )

    # Sort orbit_members by orbit_id and time
    orbit_members_table = (
        orbit_members.flattened_table()
        .drop_columns(
            [
                "residuals.values",
                "residuals.chi2",
                "residuals.dof",
                "residuals.probability",
                "solution",
                "outlier",
            ]
        )
        .join(
            observations.flattened_table()
            .select(["id", "time.days", "time.nanos", "observatory_code"])
            .combine_chunks(),
            "obs_id",
            "id",
        )
    )

    # Handle deep drilling observations (if any)
    if max_obs_per_night is not None:
        dds = reduce_deep_drilling_observations(
            orbit_members, observations, max_obs_per_night=max_obs_per_night
        )

        num_dd_orbits = len(dds.orbit_id.unique())
        num_dd_observations = len(dds.select("keep", False).obs_id.unique())
        print("Orbits with deep drilling observations removed:", num_dd_orbits)
        print("Deep drilling observations removed:", num_dd_observations)

        orbit_members_table = orbit_members_table.join(
            dds.table.select(["obs_id", "keep"]).rename_columns(
                ["obs_id", "inv_deep_drilling_filtered"]
            ),
            "obs_id",
            "obs_id",
        )
        orbit_members_table = orbit_members_table.append_column(
            "deep_drilling_filtered_temp",
            pc.invert(
                orbit_members_table["inv_deep_drilling_filtered"].combine_chunks()
            ),
        )
        # Orbits that did not have deep drilling observations are not in the dds table
        # so we need to set their null values to False
        orbit_members_table = orbit_members_table.append_column(
            "deep_drilling_filtered",
            pc.fill_null(orbit_members_table["deep_drilling_filtered_temp"], False),
        )

    else:
        orbit_members_table = orbit_members_table.append_column(
            "deep_drilling_filtered",
            pa.array([False] * len(orbit_members_table), type=pa.bool_()),
        )

    # If MPC crossmatch is provided, filter the crossmatch to see
    # if we have any potential orbit member observations that are within
    # 2 seconds and 2 arcseconds of an MPC observation. If so, we will
    # need to submit these observations as "ITF-ITF" idenfications.
    if mpc_crossmatch is not None:

        mpc_crossmatch_filtered = mpc_crossmatch.apply_mask(
            pc.and_(
                pc.is_in(mpc_crossmatch.obs_id, orbit_members_table["obs_id"]),
                pc.and_(
                    pc.less_equal(
                        pc.abs(mpc_crossmatch.time_difference), 2 * 1 / 86400
                    ),
                    pc.less_equal(mpc_crossmatch.distance, 2 / 3600),
                ),
            )
        )

        if len(mpc_crossmatch_filtered) > 0:

            print(
                f"{len(mpc_crossmatch_filtered)} observations are within 2 seconds and 2 arcseconds of an MPC observation"
            )

            not_itf_mask = pc.invert(pc.equal(mpc_crossmatch_filtered.status, "I"))
            if pc.any(not_itf_mask).as_py():
                raise ValueError(
                    "MPC observations are not all ITF observations: ",
                    mpc_crossmatch_filtered.apply_mask(not_itf_mask).to_dataframe()[:5],
                )

            orbit_members_table = orbit_members_table.join(
                mpc_crossmatch_filtered.table.select(
                    ["obs_id", "mpc_id", "trksub"]
                ).rename_columns(["obs_id", "mpc_obs_id", "mpc_trksub"]),
                "obs_id",
                "obs_id",
            )

    if "mpc_obs_id" not in orbit_members_table.column_names:
        orbit_members_table = orbit_members_table.append_column(
            "mpc_obs_id",
            pa.array([None] * len(orbit_members_table), type=pa.large_string()),
        )

    orbit_members_table = orbit_members_table.sort_by(
        [
            ("orbit_id", "ascending"),
            ("time.days", "ascending"),
            ("time.nanos", "ascending"),
            ("observatory_code", "ascending"),
        ]
    )

    # Create submission members table which spans both
    # new observations that will be submitted, observations that were filtered out
    # by the deep drilling filter, and observations that were identified as ITF observations
    submission_members = SubmissionMembers.from_kwargs(
        orbit_id=orbit_members_table["orbit_id"],
        trksub=orbit_id_to_trksub(orbit_members_table["orbit_id"]),
        obssubid=orbit_members_table["obs_id"],
        submission_id=pa.repeat(submission_id, len(orbit_members_table)),
        itf_obs_id=orbit_members_table["mpc_obs_id"],
        deep_drilling_filtered=orbit_members_table["deep_drilling_filtered"],
        submitted=pa.repeat(False, len(orbit_members_table)),
    )

    # Now remove the deep drilling filtered observations so we can split into
    # new observations and ITF observations
    submission_members_filtered = submission_members.select(
        "deep_drilling_filtered", False
    )

    # Select only the observations that are in the ITF and
    # create an Identifications table
    identifications_mask = pc.invert(pc.is_null(submission_members_filtered.itf_obs_id))
    if len(submission_members_filtered.apply_mask(identifications_mask)) > 0:

        identifications_orbits = submission_members_filtered.apply_mask(
            identifications_mask
        ).orbit_id.unique()
        identification_members = orbit_members_table.filter(
            pc.is_in(orbit_members_table["orbit_id"], pa.array(identifications_orbits))
        )
        identification_members_filtered = identification_members.filter(
            pc.invert(
                pc.is_in(
                    identification_members["obs_id"],
                    submission_members.select("deep_drilling_filtered", True).obssubid,
                )
            )
        )

        identifications = Identifications.from_kwargs(
            submission_id=pa.repeat(
                submission_id, len(identification_members_filtered)
            ),
            orbit_id=identification_members_filtered["orbit_id"],
            trksub=orbit_id_to_trksub(identification_members_filtered["orbit_id"]),
            obs_id=identification_members_filtered["obs_id"],
            mpc_obs_id=identification_members_filtered["mpc_obs_id"],
            mpc_trksub=identification_members_filtered["mpc_trksub"],
            days=identification_members_filtered["time.days"],
            nanos=identification_members_filtered["time.nanos"],
            observatory_code=identification_members_filtered["observatory_code"],
        )

    else:
        identifications = Identifications.empty()

    # Select only the observations that are not in the ITF and will
    # be submitted as new astrometry in the ADES format
    submission_members_new = submission_members_filtered.apply_mask(
        pc.invert(
            pc.is_in(submission_members_filtered.obssubid, identifications.itf().obs_id)
        )
    )

    # Sort observations by orbit_id and time
    submission_members_table = (
        submission_members_new.flattened_table()
        .join(observations.flattened_table(), "obssubid", "id")
        .sort_by(
            [
                ("orbit_id", "ascending"),
                ("time.days", "ascending"),
                ("time.nanos", "ascending"),
                ("observatory_code", "ascending"),
            ]
        )
    )

    # Create ADES observations
    ades = ADESObservations.from_kwargs(
        permID=None,
        provID=None,
        trkSub=submission_members_table["trksub"],
        obsSubID=submission_members_table["obssubid"],
        obsTime=Timestamp.from_kwargs(
            days=submission_members_table["time.days"],
            nanos=submission_members_table["time.nanos"],
            scale="utc",
        ),
        ra=submission_members_table["ra"],
        dec=submission_members_table["dec"],
        rmsRA=pc.divide(submission_members_table["ra_sigma"], 3600.0),
        rmsDec=pc.divide(submission_members_table["dec_sigma"], 3600.0),
        mag=submission_members_table["mag"],
        rmsMag=submission_members_table["mag_sigma"],
        band=submission_members_table["filter"],
        stn=submission_members_table["observatory_code"],
        mode=pa.repeat("CCD", len(submission_members_table)),
        astCat=pa.repeat(astrometric_catalog, len(submission_members_table)),
        remarks=None,
    )

    assert len(submission_members) == len(orbit_members)

    return submission_members, ades, identifications
