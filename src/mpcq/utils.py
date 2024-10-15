import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import quivr as qv
from adam_core.observations import SourceCatalog
from adam_core.observers.utils import calculate_observing_night
from adam_core.orbit_determination import FittedOrbitMembers


class DeepDrillingSummary(qv.Table):
    orbit_id = qv.LargeStringColumn()
    obs_id = qv.LargeStringColumn()
    night = qv.Int64Column(nullable=True)
    mjd = qv.Float64Column(nullable=True)
    keep = qv.BooleanColumn()


def reduce_deep_drilling_observations(
    orbit_members: FittedOrbitMembers,
    observations: SourceCatalog,
    max_obs_per_night: int = 6,
) -> DeepDrillingSummary:
    """
    Submitting many back-to-back observations within a single night (such as those observed during deep drilling)
    is discouraged by the MPC. This function reduces the number of observations to a maximum of `max_obs_per_night`
    per night for each orbit. The observations are reduced by selecting a subset of observations that are evenly
    distributed through the night as to attain the best possible coverage of the orbit's motion.

    Parameters
    ----------
    orbit_members : OrbitMembers
        Orbit members.
    observations : SourceCatalog
        Observations as a SourceCatalog.
    max_obs_per_night : int
        Maximum number of observations per night for each orbit, by default 6.

    Returns
    -------
    DeepDrillingSummary
        Table of observations reduced to a maximum of `max_obs_per_night` per night for each orbit.
    """
    observation_nights = (
        observations.flattened_table()
        .append_column(
            "night",
            calculate_observing_night(observations.observatory_code, observations.time),
        )
        .append_column("mjd", observations.time.mjd())
        .select(["id", "night", "mjd"])
    )

    members_table = orbit_members.flattened_table().drop_columns(["residuals.values"])

    percentiles = np.linspace(0, 1, 6)
    members_table = members_table.join(observation_nights, "obs_id", "id")
    members_table_grouped = members_table.group_by(["orbit_id", "night"]).aggregate(
        [("night", "count")]
    )
    members_table_grouped = members_table_grouped.sort_by(
        [
            ("orbit_id", "ascending"),
            ("night", "ascending"),
        ]
    )

    # Find orbits that have more than max_obs_per_night observations in
    # a single night
    deep_drilling_orbits = members_table_grouped.filter(
        pc.greater(members_table_grouped["night_count"], max_obs_per_night)
    )["orbit_id"].unique()

    # Compute percentiles
    percentiles = np.linspace(0, 100, max_obs_per_night)

    dds = DeepDrillingSummary.empty()
    for orbit in deep_drilling_orbits:

        orbit_members_summary = members_table.filter(
            pc.equal(members_table["orbit_id"], orbit)
        )
        orbit_members_grouped_summary = members_table_grouped.filter(
            pc.equal(members_table_grouped["orbit_id"], orbit)
        )

        deep_drilling_nights = orbit_members_grouped_summary.filter(
            pc.greater(orbit_members_grouped_summary["night_count"], max_obs_per_night)
        )["night"].unique()

        for night in deep_drilling_nights:

            # Extract MJDs and observation IDs
            orbit_members_summary_night = orbit_members_summary.filter(
                pc.equal(orbit_members_summary["night"], night)
            )
            mjds = orbit_members_summary_night["mjd"].to_numpy(zero_copy_only=False)
            obs_ids = orbit_members_summary_night["obs_id"].to_numpy(
                zero_copy_only=False
            )

            # Compute percentiles for these MJDs
            percentile_values = np.percentile(mjds, percentiles)

            # Bin map the MJDs to the percentiles
            mapped = np.digitize(mjds, percentile_values)

            # Find the indices of the percentiles in the original array
            idx = np.empty_like(percentiles, dtype=int)
            for p_i in range(len(idx)):
                idx[p_i] = np.where(mapped == (p_i + 1))[0][0]

            # Create a DeepDrillingSummary object
            dds_night = DeepDrillingSummary.from_kwargs(
                orbit_id=pa.repeat(orbit, len(obs_ids)),
                obs_id=obs_ids,
                night=pa.repeat(night, len(obs_ids)),
                mjd=mjds,
                keep=np.isin(np.arange(len(obs_ids)), idx),
            )

            dds = qv.concatenate([dds, dds_night])

    return dds
