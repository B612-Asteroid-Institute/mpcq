import os

import pytest

from ..client import MPCObservationsClient


@pytest.mark.integration
@pytest.mark.skipif(
    "MPCQ_INTEGRATION_TESTS" not in os.environ,
    reason="MPCQ_INTEGRATION_TESTS must be set",
)
class TestIntegration:
    def test_connection(self):
        # Test that a connection can be established
        MPCObservationsClient.connect_using_gcloud()

    def test_get_object_observations(self, mpc_client):
        observations = list(mpc_client.get_object_observations("2022 AJ2"))
        assert len(observations) >= 10
        for o in observations:
            assert o.unpacked_provisional_designation == "2022 AJ2"

    def test_get_object_observations_filter_by_obscode(self, mpc_client):
        observations = mpc_client.get_object_observations("2022 AJ2", obscode="I52")
        observations = list(observations)
        assert len(observations) > 0
        for o in observations:
            assert o.unpacked_provisional_designation == "2022 AJ2"
            assert o.obscode == "I52"

    def test_get_object_observations_filter_by_filter_band(self, mpc_client):
        observations = mpc_client.get_object_observations("2022 AJ2", filter_band="G")
        observations = list(observations)
        assert len(observations) > 0
        for o in observations:
            assert o.unpacked_provisional_designation == "2022 AJ2"
            assert o.filter_band == "G"

    def test_get_object_submissions(self, mpc_client):
        submissions = list(mpc_client.get_object_submissions("2022 AJ2"))
        assert len(submissions) >= 1
        for s in submissions:
            assert s.num_observations >= 1

        observations = mpc_client.get_object_observations("2022 AJ2")
        observations = list(observations)

        num_observations_from_submissions = sum(s.num_observations for s in submissions)
        num_observations = len(observations)
        assert num_observations_from_submissions == num_observations


@pytest.fixture
def mpc_client():
    client = MPCObservationsClient.connect_using_gcloud()
    yield client
    client.close()
