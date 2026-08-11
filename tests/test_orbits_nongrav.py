import logging

import numpy as np
from adam_core.orbits.non_gravitational_parameters import MARSDEN_STANDARD_CONSTANTS
from adam_core.time import Timestamp

from mpcq.orbits import MPCOrbits


def test_mpcq_orbits_maps_nongrav_parameters_into_adam_core():
    # a1/a2/a3 (and their uncertainties) are reported by the MPC in units of
    # 1e-10 au/d^2; adam_core's canonical A1/A2/A3 are in au/d^2.
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
        a1=[None, 1.2e-2],
        a2=[-8.7e-4, None],
        a3=[None, 3.4e-4],
        a1_unc=[None, 2.0e-3],
        a2_unc=[1.1e-4, None],
        a3_unc=[None, 5.0e-5],
        h=[18.0, 19.0],
        g=[0.15, 0.25],
        created_at=Timestamp.from_mjd([60000.0, 60001.0], scale="tdb"),
        updated_at=Timestamp.from_mjd([60000.0, 60001.0], scale="tdb"),
    )

    orbits = mpc_orbits.orbits()

    assert len(orbits) == 2
    nongrav = orbits.non_gravitational_parameters
    assert nongrav.source.to_pylist() == ["MPCQ", "MPCQ"]
    np.testing.assert_allclose(
        nongrav.A2.to_numpy(zero_copy_only=False)[0],
        -8.7e-14,
    )
    np.testing.assert_allclose(
        nongrav.A1.to_numpy(zero_copy_only=False)[1],
        1.2e-12,
    )
    np.testing.assert_allclose(
        nongrav.A3.to_numpy(zero_copy_only=False)[1],
        3.4e-14,
    )

    # The MPC's a1/a2/a3 columns are comet-model fits, so rows carrying them
    # get the standard Marsden g(r) constants (null constants would select
    # the asteroid (1 au / r)^2 convention instead).
    for name, value in MARSDEN_STANDARD_CONSTANTS.items():
        assert getattr(nongrav, name).to_pylist() == [value, value]

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
    # source and the Marsden constants), matching adam_core's SBDB/NEOCC
    # importers, and the coordinate covariance must stay a plain 6x6
    # without the non-grav block.
    nongrav = orbits.non_gravitational_parameters
    assert nongrav.source[0].as_py() is None
    assert nongrav.A1[0].as_py() is None
    assert nongrav.A2[0].as_py() is None
    assert nongrav.A3[0].as_py() is None
    for name in MARSDEN_STANDARD_CONSTANTS:
        assert getattr(nongrav, name)[0].as_py() is None
    assert not orbits.coordinates.covariance.has_nongrav_block()


def test_mpcq_orbits_warns_on_unsupported_nongrav_parameters(caplog):
    # dt (Yeomans-Chodas time offset), yarkovsky, and srp are not
    # representable in adam_core's canonical schema: their values are
    # dropped with a warning naming the affected objects.
    mpc_orbits = MPCOrbits.from_kwargs(
        requested_provid=["test-d"],
        primary_designation=["test-d"],
        id=[4],
        provid=["test-d"],
        epoch=Timestamp.from_mjd([60000.0], scale="tdb"),
        q=[1.2],
        e=[0.6],
        i=[11.0],
        node=[335.0],
        argperi=[325.0],
        peri_time=[60132.0],
        q_unc=[0.01],
        e_unc=[0.001],
        i_unc=[0.1],
        node_unc=[0.1],
        argperi_unc=[0.1],
        peri_time_unc=[0.5],
        a1=[1.07e1],
        a2=[-3.6e-1],
        dt=[45.7],
        yarkovsky=[2.0e-2],
        srp=[0.5],
        h=[20.0],
        g=[0.15],
        created_at=Timestamp.from_mjd([60000.0], scale="tdb"),
        updated_at=Timestamp.from_mjd([60000.0], scale="tdb"),
    )

    with caplog.at_level(logging.WARNING, logger="mpcq.orbits"):
        orbits = mpc_orbits.orbits()

    warned = " ".join(record.message for record in caplog.records)
    assert "'dt'" in warned
    assert "'yarkovsky'" in warned
    assert "'srp'" in warned
    assert "test-d" in warned

    # The supported A-values still come through, scaled and stamped.
    nongrav = orbits.non_gravitational_parameters
    np.testing.assert_allclose(nongrav.A1.to_numpy(zero_copy_only=False)[0], 1.07e-9)
    np.testing.assert_allclose(nongrav.A2.to_numpy(zero_copy_only=False)[0], -3.6e-11)
    for name, value in MARSDEN_STANDARD_CONSTANTS.items():
        assert getattr(nongrav, name)[0].as_py() == value
