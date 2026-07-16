import numpy as np
from adam_core.time import Timestamp

from mpcq.orbits import MPCOrbits


def test_mpcq_orbits_maps_nongrav_parameters_into_adam_core():
    mpc_orbits = MPCOrbits.from_kwargs(
        requested_provid=["test-a", "test-b"],
        primary_designation=["test-a", "test-b"],
        id=[1, 2],
        provid=["test-a", "test-b"],
        epoch=Timestamp.from_mjd([60000.0, 60001.0], scale="tdb"),
        q=[0.8, 1.1],
        e=[0.2, 0.1],
        i=[5.0, 3.0],
        node=[120.0, 130.0],
        argperi=[45.0, 50.0],
        peri_time=[59980.0, 59990.0],
        q_unc=[0.01, 0.02],
        e_unc=[0.001, 0.002],
        i_unc=[0.1, 0.2],
        node_unc=[0.1, 0.2],
        argperi_unc=[0.1, 0.2],
        peri_time_unc=[0.5, 0.6],
        a1=[None, 1.2e-12],
        a2=[-8.7e-14, None],
        a3=[None, 3.4e-14],
        a1_unc=[None, 2.0e-13],
        a2_unc=[1.1e-14, None],
        a3_unc=[None, 5.0e-15],
        h=[18.0, 19.0],
        g=[0.15, 0.25],
        created_at=Timestamp.from_mjd([60000.0, 60001.0], scale="tdb"),
        updated_at=Timestamp.from_mjd([60000.0, 60001.0], scale="tdb"),
    )

    orbits = mpc_orbits.orbits()

    assert len(orbits) == 2
    assert orbits.non_gravitational_parameters.source.to_pylist() == ["MPCQ", "MPCQ"]
    np.testing.assert_allclose(
        orbits.non_gravitational_parameters.A2.to_numpy(zero_copy_only=False)[0],
        -8.7e-14,
    )
    np.testing.assert_allclose(
        orbits.non_gravitational_parameters.A1.to_numpy(zero_copy_only=False)[1],
        1.2e-12,
    )
    np.testing.assert_allclose(
        orbits.non_gravitational_parameters.A3.to_numpy(zero_copy_only=False)[1],
        3.4e-14,
    )

    # A-parameter uncertainties live in the coordinate covariance, which
    # extends to adam_core's fixed 9x9 layout (coordinates, A1, A2, A3).
    # The A-block diagonal is invariant under the cometary -> Cartesian
    # transform, so the reported uncertainties can be read back directly.
    covariance = orbits.coordinates.covariance
    assert covariance.nongrav_block_mask().tolist() == [True, True]
    full = covariance.to_full_matrix()
    np.testing.assert_allclose(np.sqrt(full[0, 7, 7]), 1.1e-14)
    np.testing.assert_allclose(np.sqrt(full[1, 6, 6]), 2.0e-13)
    np.testing.assert_allclose(np.sqrt(full[1, 8, 8]), 5.0e-15)
    # Parameters without a reported uncertainty are held fixed (zero rows).
    assert np.all(full[0, 6, :] == 0.0)
    assert np.all(full[0, 8, :] == 0.0)
    assert np.all(full[1, 7, :] == 0.0)


def test_mpcq_orbits_without_nongrav_values_stay_null():
    mpc_orbits = MPCOrbits.from_kwargs(
        requested_provid=["test-c"],
        primary_designation=["test-c"],
        id=[3],
        provid=["test-c"],
        epoch=Timestamp.from_mjd([60000.0], scale="tdb"),
        q=[0.9],
        e=[0.3],
        i=[10.0],
        node=[100.0],
        argperi=[40.0],
        peri_time=[59970.0],
        q_unc=[0.01],
        e_unc=[0.001],
        i_unc=[0.1],
        node_unc=[0.1],
        argperi_unc=[0.1],
        peri_time_unc=[0.5],
        h=[20.0],
        g=[0.15],
        created_at=Timestamp.from_mjd([60000.0], scale="tdb"),
        updated_at=Timestamp.from_mjd([60000.0], scale="tdb"),
    )

    orbits = mpc_orbits.orbits()

    # Rows without any non-grav values must stay fully null (including
    # source), matching adam_core's SBDB/NEOCC importers, and the coordinate
    # covariance must stay a plain 6x6 without the non-grav block.
    nongrav = orbits.non_gravitational_parameters
    assert nongrav.source[0].as_py() is None
    assert nongrav.A1[0].as_py() is None
    assert nongrav.A2[0].as_py() is None
    assert nongrav.A3[0].as_py() is None
    assert not orbits.coordinates.covariance.has_nongrav_block()
