import numpy as np

from mpcq.orbits import MPCOrbits

from adam_core.time import Timestamp


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
    assert orbits.non_gravitational_parameters.model.to_pylist() == [
        "nongrav",
        "nongrav",
    ]
    assert orbits.non_gravitational_parameters.estimated_parameter_names.to_pylist() == [
        "A2",
        "A1,A3",
    ]
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
    np.testing.assert_allclose(
        orbits.non_gravitational_parameters.A2_sigma.to_numpy(zero_copy_only=False)[0],
        1.1e-14,
    )
    np.testing.assert_allclose(
        orbits.non_gravitational_parameters.A1_sigma.to_numpy(zero_copy_only=False)[1],
        2.0e-13,
    )
    np.testing.assert_allclose(
        orbits.non_gravitational_parameters.A3_sigma.to_numpy(zero_copy_only=False)[1],
        5.0e-15,
    )


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
    # source), matching adam_core's SBDB/NEOCC importers; downstream
    # propagators treat metadata-only rows differently from null rows.
    nongrav = orbits.non_gravitational_parameters
    assert nongrav.source[0].as_py() is None
    assert nongrav.model[0].as_py() is None
    assert nongrav.solution_dimension[0].as_py() is None
    assert nongrav.A1[0].as_py() is None
