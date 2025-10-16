import pyarrow as pa
import pyarrow.compute as pc
import pytest

from ..utils import orbit_id_to_trksub


def test_orbit_id_to_trksub():
    # Test that orbit_id_to_trksub returns the correct trksub values
    orbit_ids = pa.array(
        ["073273eff323476e8dfa3faac8c0fd45", "0453e2948770490abd03d3ba2bd2df07"],
        type=pa.large_string(),
    )

    actual_trksubs = orbit_id_to_trksub(orbit_ids)
    desired_trksubs = pa.array(["tc0fd45", "td2df07"], type=pa.large_string())

    assert pc.all(pc.equal(desired_trksubs, actual_trksubs)).as_py()


def test_orbit_id_to_trksub_raises():
    # Test that orbit_id_to_trksub returns the correct trksub values
    orbit_ids = pa.array(
        ["dfa3faac8c0fd45", "0453e294877049f07"], type=pa.large_string()
    )

    with pytest.raises(AssertionError):
        orbit_id_to_trksub(orbit_ids)
