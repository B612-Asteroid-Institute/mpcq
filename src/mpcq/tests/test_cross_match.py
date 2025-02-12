from pathlib import Path

import pyarrow as pa
import pytest
from adam_core.observations import ADESObservations
from adam_core.time import Timestamp
from astropy.time import Time
from google.cloud import bigquery

from mpcq.client import BigQueryMPCClient
from mpcq.observations import CrossMatchedMPCObservations

TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def mock_bigquery_client(mocker):
    # Mock the BigQuery client
    mock_client = mocker.Mock(spec=bigquery.Client)

    # Mock query job results
    mock_query_job = mocker.Mock()
    mock_query_job.result = mocker.Mock()
    mock_client.query = mocker.Mock(return_value=mock_query_job)

    return mock_client


@pytest.fixture
def test_ades_observations():
    # Create sample ADES observations for testing
    obstime = Time(
        ["2023-01-01T00:00:00", "2023-01-01T00:10:00", "2023-01-01T00:20:00"],
        format="isot",
        scale="utc",
    )

    return ADESObservations.from_kwargs(
        obsTime=Timestamp.from_astropy(obstime),
        ra=[10.0, 10.1, 10.2],
        dec=[20.0, 20.1, 20.2],
        stn=["F51", "F51", "F51"],
        obsSubID=["test1", "test2", "test3"],
        mode=["test1", "test2", "test3"],
        astCat=["test1", "test2", "test3"],
    )


def test_cross_match_observations_empty_result(
    mock_bigquery_client, test_ades_observations
):
    # Setup mock to return empty results
    mock_bigquery_client.query.return_value.result.return_value.to_arrow.return_value = pa.table(
        {
            "input_id": pa.array([]),
            "obs_id": pa.array([]),
            "separation_meters": pa.array([]),
            "separation_seconds": pa.array([]),
        }
    )

    client = BigQueryMPCClient()
    client.client = mock_bigquery_client

    result = client.cross_match_observations(test_ades_observations)
    assert isinstance(result, CrossMatchedMPCObservations)
    assert len(result) == 0


def test_cross_match_observations_with_matches(
    mock_bigquery_client, test_ades_observations
):
    # Load test data from parquet files
    matched_results = pa.parquet.read_table(TEST_DATA_DIR / "matched_results.parquet")
    final_results = pa.parquet.read_table(TEST_DATA_DIR / "final_results.parquet")

    # Setup mock to return our test data
    mock_bigquery_client.query.return_value.result.return_value.to_arrow.side_effect = [
        matched_results,
        final_results,
    ]

    client = BigQueryMPCClient()
    client.client = mock_bigquery_client

    result = client.cross_match_observations(test_ades_observations)

    assert isinstance(result, CrossMatchedMPCObservations)
    assert len(result) > 0
    assert "separation_arcseconds" in result.table.column_names
    assert "separation_seconds" in result.table.column_names
    assert "mpc_observations" in result.table.column_names


def test_cross_match_observations_invalid_input(mock_bigquery_client):
    # Create ADES observations with null obsSubID
    obstime = Time(["2023-01-01T00:00:00"], format="isot", scale="utc")
    invalid_observations = ADESObservations.from_kwargs(
        obsTime=Timestamp.from_astropy(obstime),
        ra=[10.0],
        dec=[20.0],
        stn=["F51"],
        obsSubID=[None],
        mode=["test1"],
        astCat=["test1"],
    )

    client = BigQueryMPCClient()
    client.client = mock_bigquery_client

    with pytest.raises(AssertionError):
        client.cross_match_observations(invalid_observations)
