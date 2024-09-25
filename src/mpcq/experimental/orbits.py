import numpy as np
import pyarrow as pa
import quivr as qv
from adam_core.coordinates import CometaryCoordinates, CoordinateCovariances, Origin
from adam_core.orbits import Orbits
from adam_core.time import Timestamp


class MPCOrbits(qv.Table):

    id = qv.Int64Column()
    primary_designation = qv.LargeStringColumn()
    provid = qv.LargeStringColumn(nullable=True)
    permid = qv.LargeStringColumn(nullable=True)
    epoch = Timestamp.as_column()
    q = qv.Float64Column(nullable=True)
    e = qv.Float64Column(nullable=True)
    i = qv.Float64Column(nullable=True)
    node = qv.Float64Column(nullable=True)
    argperi = qv.Float64Column(nullable=True)
    peri_time = qv.Float64Column(nullable=True)
    q_unc = qv.Float64Column(nullable=True)
    e_unc = qv.Float64Column(nullable=True)
    i_unc = qv.Float64Column(nullable=True)
    node_unc = qv.Float64Column(nullable=True)
    argperi_unc = qv.Float64Column(nullable=True)
    peri_time_unc = qv.Float64Column(nullable=True)
    a1 = qv.Float64Column(nullable=True)
    a2 = qv.Float64Column(nullable=True)
    a3 = qv.Float64Column(nullable=True)
    h = qv.Float64Column(nullable=True)
    g = qv.Float64Column(nullable=True)
    created_at = Timestamp.as_column()
    updated_at = Timestamp.as_column()

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
