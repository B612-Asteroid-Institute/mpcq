from pathlib import Path

import pyarrow as pa
import pytest
from adam_core.observations import ADESObservations
from adam_core.time import Timestamp
from astropy.time import Time
from google.cloud import bigquery
from pytest_mock import MockFixture

from mpcq.client import BigQueryMPCClient
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
        obsTime=Timestamp.from_astropy(obstime),
        ra=[10.0, 10.1, 10.2],
        dec=[20.0, 20.1, 20.2],
        stn=["F51", "F51", "F51"],
        obsSubID=["test1", "test2", "test3"],
        mode=["test1", "test2", "test3"],
        astCat=["test1", "test2", "test3"],
    )


@pytest.fixture
def test_dataset_ids() -> tuple[str, str]:
    return "test_dataset", "test_views_dataset"


def test_cross_match_observations_empty_result(
    mocker: MockFixture,
    test_ades_observations: ADESObservations,
    test_dataset_ids: tuple[str, str],
) -> None:
    # Create mock client and query job
    mock_client = mocker.Mock(spec=bigquery.Client)
    mock_query_job = mocker.Mock()
    mock_query_job.result.return_value.to_arrow.return_value = pa.table(
        {
            "input_id": pa.array([]),
            "obs_id": pa.array([]),
            "separation_meters": pa.array([]),
            "separation_seconds": pa.array([]),
        }
    )
    mock_client.query.return_value = mock_query_job

    # Patch the BigQuery client
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    dataset_id, views_dataset_id = test_dataset_ids
    client = BigQueryMPCClient(dataset_id=dataset_id, views_dataset_id=views_dataset_id)
    result = client.cross_match_observations(test_ades_observations)
    assert isinstance(result, CrossMatchedMPCObservations)
    assert len(result) == 0


def test_cross_match_observations_with_matches(
    mocker: MockFixture,
    test_ades_observations: ADESObservations,
    test_dataset_ids: tuple[str, str],
) -> None:
    # Create mock client and query job
    mock_client = mocker.Mock(spec=bigquery.Client)
    mock_query_job = mocker.Mock()

    # Load test data from parquet files
    matched_results = pa.parquet.read_table(TEST_DATA_DIR / "matched_results.parquet")
    final_results = pa.parquet.read_table(TEST_DATA_DIR / "final_results.parquet")

    # Setup mock to return our test data
    mock_query_job.result.return_value.to_arrow.side_effect = [
        matched_results,
        final_results,
    ]
    mock_client.query.return_value = mock_query_job

    # Patch the BigQuery client
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    dataset_id, views_dataset_id = test_dataset_ids
    client = BigQueryMPCClient(dataset_id=dataset_id, views_dataset_id=views_dataset_id)
    result = client.cross_match_observations(test_ades_observations)

    assert isinstance(result, CrossMatchedMPCObservations)
    assert len(result) > 0
    assert "separation_arcseconds" in result.table.column_names
    assert "separation_seconds" in result.table.column_names
    assert "mpc_observations" in result.table.column_names


def test_cross_match_observations_invalid_input(
    mocker: MockFixture,
    test_dataset_ids: tuple[str, str],
) -> None:
    # Create mock client
    mock_client = mocker.Mock(spec=bigquery.Client)
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

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

    dataset_id, views_dataset_id = test_dataset_ids
    client = BigQueryMPCClient(dataset_id=dataset_id, views_dataset_id=views_dataset_id)
    with pytest.raises(AssertionError):
        client.cross_match_observations(invalid_observations)
