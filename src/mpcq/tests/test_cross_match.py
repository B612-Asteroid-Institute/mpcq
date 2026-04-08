from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
from adam_core.observations import ADESObservations
from adam_core.time import Timestamp
from astropy.time import Time
from google.cloud import bigquery
from pytest_mock import MockFixture

from mpcq.client import METERS_PER_ARCSECONDS, BigQueryMPCClient
from mpcq.observations import CrossMatchedMPCObservations

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def test_ades_observations() -> ADESObservations:
    # Create sample ADES observations for testing
    obstime = Time(
        ["2023-01-01T00:00:00", "2023-01-01T00:10:00", "2023-01-01T00:20:00"],
        format="isot",
        scale="utc",
    )

    return ADESObservations.from_kwargs(
        obsTime=Timestamp.from_iso8601(obstime.utc.isot, scale="utc"),
        ra=[10.0, 10.1, 10.2],
        dec=[20.0, 20.1, 20.2],
        stn=["F51", "F51", "F51"],
        obsSubID=["test1", "test2", "test3"],
        mode=["test1", "test2", "test3"],
        astCat=["test1", "test2", "test3"],
    )


@pytest.fixture
def test_dataset_id() -> str:
    return "test_dataset"


def test_cross_match_observations_empty_result(
    mocker: MockFixture,
    test_ades_observations: ADESObservations,
    test_dataset_id: str,
) -> None:
    mock_client = mocker.Mock(spec=bigquery.Client)
    mock_query_job = mocker.Mock()
    mock_query_job.result.return_value.to_arrow.return_value = pa.table(
        {
            "input_id": pa.array([]),
            "separation_meters": pa.array([]),
            "separation_seconds": pa.array([]),
        }
    )
    mock_client.query.return_value = mock_query_job
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    client = BigQueryMPCClient(dataset_id=test_dataset_id)
    result = client.cross_match_observations(test_ades_observations)
    assert isinstance(result, CrossMatchedMPCObservations)
    assert len(result) == 0


def test_cross_match_observations_with_matches(
    mocker: MockFixture,
    test_ades_observations: ADESObservations,
    test_dataset_id: str,
) -> None:
    mock_client = mocker.Mock(spec=bigquery.Client)
    mock_query_job = mocker.Mock()

    final_results = pa.parquet.read_table(TEST_DATA_DIR / "final_results.parquet")

    mock_query_job.result.return_value.to_arrow.return_value = final_results
    mock_client.query.return_value = mock_query_job
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    client = BigQueryMPCClient(dataset_id=test_dataset_id)
    result = client.cross_match_observations(test_ades_observations)

    assert isinstance(result, CrossMatchedMPCObservations)
    assert len(result) > 0
    assert "separation_arcseconds" in result.table.column_names
    assert "separation_seconds" in result.table.column_names
    assert "mpc_observations" in result.table.column_names
    np.testing.assert_allclose(
        result.table["separation_arcseconds"].to_numpy(zero_copy_only=False),
        final_results["separation_meters"].to_numpy(zero_copy_only=False) / METERS_PER_ARCSECONDS,
    )


def test_cross_match_query_uses_literal_bounds_and_projection(
    mocker: MockFixture,
    test_ades_observations: ADESObservations,
    test_dataset_id: str,
) -> None:
    mock_client = mocker.Mock(spec=bigquery.Client)
    mock_query_job = mocker.Mock()
    mock_query_job.result.return_value.to_arrow.return_value = pa.table(
        {
            "input_id": pa.array([]),
            "separation_meters": pa.array([]),
            "separation_seconds": pa.array([]),
        }
    )
    mock_client.query.return_value = mock_query_job
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    client = BigQueryMPCClient(dataset_id=test_dataset_id)
    client.cross_match_observations(test_ades_observations)

    assert mock_client.query.call_count == 1
    query = mock_client.query.call_args.args[0]
    assert "obs.stn IN ('F51')" in query
    assert "obs.obstime BETWEEN TIMESTAMP('" in query
    assert "SAFE_CAST(obs.ra AS FLOAT64)" in query
    assert "SAFE_CAST(obs.dec AS FLOAT64)" in query
    assert "obs.*" not in query


def test_cross_match_observations_buckets_months(
    mocker: MockFixture,
    test_dataset_id: str,
) -> None:
    mock_client = mocker.Mock(spec=bigquery.Client)
    mock_query_job = mocker.Mock()
    mock_query_job.result.return_value.to_arrow.return_value = pa.table(
        {
            "input_id": pa.array([]),
            "separation_meters": pa.array([]),
            "separation_seconds": pa.array([]),
        }
    )
    mock_client.query.return_value = mock_query_job
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    obstime = Time(["2023-01-15T00:00:00", "2023-02-15T00:00:00"], format="isot", scale="utc")
    observations = ADESObservations.from_kwargs(
        obsTime=Timestamp.from_iso8601(obstime.utc.isot, scale="utc"),
        ra=[10.0, 11.0],
        dec=[20.0, 21.0],
        stn=["F51", "F51"],
        obsSubID=["jan", "feb"],
        mode=["test1", "test2"],
        astCat=["test1", "test2"],
    )

    client = BigQueryMPCClient(dataset_id=test_dataset_id)
    client.cross_match_observations(observations)

    assert mock_client.query.call_count == 2


def test_cross_match_observations_invalid_input(
    mocker: MockFixture,
    test_dataset_id: str,
) -> None:
    mock_client = mocker.Mock(spec=bigquery.Client)
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    obstime = Time(["2023-01-01T00:00:00"], format="isot", scale="utc")
    invalid_observations = ADESObservations.from_kwargs(
        obsTime=Timestamp.from_iso8601(obstime.utc.isot, scale="utc"),
        ra=[10.0],
        dec=[20.0],
        stn=["F51"],
        obsSubID=[None],
        mode=["test1"],
        astCat=["test1"],
    )

    client = BigQueryMPCClient(dataset_id=test_dataset_id)
    with pytest.raises(AssertionError):
        client.cross_match_observations(invalid_observations)
