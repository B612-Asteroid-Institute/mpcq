import numpy as np
import pyarrow as pa
import quivr as qv
from adam_core.coordinates import CometaryCoordinates, CoordinateCovariances, Origin
from adam_core.orbits import Orbits
from adam_core.time import Timestamp


class MPCOrbits(qv.Table):
    # Metadata convenience columns
    requested_provid = qv.LargeStringColumn()
    primary_designation = qv.LargeStringColumn(nullable=True)

    # Convenience alias for primary designation used throughout the codebase
    provid = qv.LargeStringColumn(nullable=True)

    # Derived epoch as Timestamp for downstream usage
    epoch = Timestamp.as_column(nullable=True)

    # All columns from moeyens-thor-dev.mpc_sbn_aurora.public_mpc_orbits
    id = qv.Int64Column(nullable=True)
    packed_primary_provisional_designation = qv.LargeStringColumn(nullable=True)
    unpacked_primary_provisional_designation = qv.LargeStringColumn(nullable=True)
    mpc_orb_jsonb = qv.LargeStringColumn(nullable=True)
    created_at = Timestamp.as_column(nullable=True)
    updated_at = Timestamp.as_column(nullable=True)
    orbit_type_int = qv.Int64Column(nullable=True)
    u_param = qv.Int64Column(nullable=True)
    nopp = qv.Int64Column(nullable=True)
    arc_length_total = qv.Float64Column(nullable=True)
    arc_length_sel = qv.Float64Column(nullable=True)
    nobs_total = qv.Int64Column(nullable=True)
    nobs_total_sel = qv.Int64Column(nullable=True)
    a = qv.Float64Column(nullable=True)
    q = qv.Float64Column(nullable=True)
    e = qv.Float64Column(nullable=True)
    i = qv.Float64Column(nullable=True)
    node = qv.Float64Column(nullable=True)
    argperi = qv.Float64Column(nullable=True)
    peri_time = qv.Float64Column(nullable=True)
    yarkovsky = qv.Float64Column(nullable=True)
    srp = qv.Float64Column(nullable=True)
    a1 = qv.Float64Column(nullable=True)
    a2 = qv.Float64Column(nullable=True)
    a3 = qv.Float64Column(nullable=True)
    dt = qv.Float64Column(nullable=True)
    mean_anomaly = qv.Float64Column(nullable=True)
    period = qv.Float64Column(nullable=True)
    mean_motion = qv.Float64Column(nullable=True)
    a_unc = qv.Float64Column(nullable=True)
    q_unc = qv.Float64Column(nullable=True)
    e_unc = qv.Float64Column(nullable=True)
    i_unc = qv.Float64Column(nullable=True)
    node_unc = qv.Float64Column(nullable=True)
    argperi_unc = qv.Float64Column(nullable=True)
    peri_time_unc = qv.Float64Column(nullable=True)
    yarkovsky_unc = qv.Float64Column(nullable=True)
    srp_unc = qv.Float64Column(nullable=True)
    a1_unc = qv.Float64Column(nullable=True)
    a2_unc = qv.Float64Column(nullable=True)
    a3_unc = qv.Float64Column(nullable=True)
    dt_unc = qv.Float64Column(nullable=True)
    mean_anomaly_unc = qv.Float64Column(nullable=True)
    period_unc = qv.Float64Column(nullable=True)
    mean_motion_unc = qv.Float64Column(nullable=True)
    epoch_mjd = qv.Float64Column(nullable=True)
    h = qv.Float64Column(nullable=True)
    g = qv.Float64Column(nullable=True)
    not_normalized_rms = qv.Float64Column(nullable=True)
    normalized_rms = qv.Float64Column(nullable=True)
    earth_moid = qv.Float64Column(nullable=True)
    fitting_datetime = Timestamp.as_column(nullable=True)
    datastream_metadata = qv.LargeStringColumn(nullable=True)

    def orbits(self) -> Orbits:
        """
        Return the orbits as an adam_core Orbits object.

        Returns
        -------
        orbits : Orbits
            The orbits and associated data for the given provisional designations.
        """
        covariances = CoordinateCovariances.from_sigmas(
            np.array(
                self.table.select(
                    [
                        "q_unc",
                        "e_unc",
                        "i_unc",
                        "node_unc",
                        "argperi_unc",
                        "peri_time_unc",
                    ]
                )
            )
        )

        # Validate required columns for conversion
        required = [
            self.id,
            self.provid,
            self.q,
            self.e,
            self.i,
            self.node,
            self.argperi,
            self.peri_time,
            self.epoch,
        ]
        if any(col is None for col in required):
            raise ValueError(
                "Missing required columns for MPCOrbits.orbits(): id, provid, q, e, i, node, argperi, peri_time, epoch"
            )

        orbits = Orbits.from_kwargs(
            orbit_id=self.id,
            object_id=self.provid,
            coordinates=CometaryCoordinates.from_kwargs(
                q=self.q,
                e=self.e,
                i=self.i,
                raan=self.node,
                ap=self.argperi,
                tp=self.peri_time,
                time=self.epoch,
                covariance=covariances,
                origin=Origin.from_kwargs(
                    code=pa.repeat("SUN", len(self)),
                ),
                frame="ecliptic",
            ).to_cartesian(),
        )
        return orbits


class MPCPrimaryObjects(qv.Table):
    requested_provid = qv.LargeStringColumn()
    primary_designation = qv.LargeStringColumn(nullable=True)
    provid = qv.LargeStringColumn(nullable=True)
    created_at = Timestamp.as_column(nullable=True)
    updated_at = Timestamp.as_column(nullable=True)
