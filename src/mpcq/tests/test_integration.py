import os
from typing import Generator

import pytest

from ..client import MPCObservationsClient


@pytest.mark.integration
@pytest.mark.skipif(
    "MPCQ_INTEGRATION_TESTS" not in os.environ,
    reason="MPCQ_INTEGRATION_TESTS must be set",
)
class TestIntegration:
    def test_connection(self) -> None:
        # Test that a connection can be established
        MPCObservationsClient.connect_using_gcloud()

    def test_get_object_observations(self, mpc_client: MPCObservationsClient) -> None:
        observations = list(mpc_client.get_object_observations("2022 AJ2"))
        assert len(observations) >= 10
        for o in observations:
            assert o.unpacked_provisional_designation == "2022 AJ2"

    def test_get_object_observations_filter_by_obscode(
        self, mpc_client: MPCObservationsClient
    ) -> None:
        observations = list(
            mpc_client.get_object_observations("2022 AJ2", obscode="I52")
        )
        assert len(observations) > 0
        for o in observations:
            assert o.unpacked_provisional_designation == "2022 AJ2"
            assert o.obscode == "I52"

    def test_get_object_observations_filter_by_filter_band(
        self, mpc_client: MPCObservationsClient
    ) -> None:
        observations = list(
            mpc_client.get_object_observations("2022 AJ2", filter_band="G")
        )
        assert len(observations) > 0
        for o in observations:
            assert o.unpacked_provisional_designation == "2022 AJ2"
            assert o.filter_band == "G"

    def test_get_object_submissions(self, mpc_client: MPCObservationsClient) -> None:
        submissions = list(mpc_client.get_object_submissions("2022 AJ2"))
        assert len(submissions) >= 1
        for s in submissions:
            assert s.num_observations >= 1

        observations = list(mpc_client.get_object_observations("2022 AJ2"))

        num_observations_from_submissions = sum(s.num_observations for s in submissions)
        num_observations = len(observations)
        assert num_observations_from_submissions == num_observations

    def test_get_orbits(self, mpc_client: MPCObservationsClient) -> None:
        orb_iter = mpc_client.orbits_chunked(chunk_size=10)
        # Do two iterations to test the chunking
        for i in range(2):
            orbits = next(orb_iter)
            assert len(orbits) >= 1
            assert len(orbits) <= 10
            for o in orbits:
                assert o.orbit_id is not None
                assert o.object_id is not None
                assert o.coordinates is not None
                assert o.coordinates.covariance.is_all_nan() is not True


@pytest.fixture
def mpc_client() -> Generator[MPCObservationsClient, None, None]:
    client = MPCObservationsClient.connect_using_gcloud()
    yield client
    client.close()
