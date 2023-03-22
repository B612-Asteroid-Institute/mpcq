

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


@pytest.fixture
def mpc_client():
    client = MPCObservationsClient.connect_using_gcloud()
    yield client
    client.close()
