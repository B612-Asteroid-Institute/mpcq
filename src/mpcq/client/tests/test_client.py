import pytest
from google.cloud import bigquery
from pytest_mock import MockFixture

from mpcq.client import BigQueryMPCClient, _normalize_string_value, _sql_string_list


def test_client_initialization(mocker: MockFixture) -> None:
    # Create mock client
    mock_client = mocker.Mock(spec=bigquery.Client)
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    # Test initialization with required dataset ID
    dataset_id = "test_dataset"
    client = BigQueryMPCClient(dataset_id=dataset_id)

    assert client.dataset_id == dataset_id


def test_client_initialization_missing_dataset_id() -> None:
    # Test initialization without required dataset_id
    with pytest.raises(TypeError, match=r".*missing.*required.*argument.*dataset_id"):
        BigQueryMPCClient()  # type: ignore


def test_client_initialization_with_kwargs(mocker: MockFixture) -> None:
    # Create mock client
    mock_client = mocker.Mock(spec=bigquery.Client)
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    # Test initialization with additional kwargs
    dataset_id = "test_dataset"
    project = "test_project"
    location = "test_location"

    client = BigQueryMPCClient(
        dataset_id=dataset_id,
        project=project,
        location=location,
    )

    assert client.dataset_id == dataset_id

    # Verify that kwargs were passed to BigQuery client
    bigquery.Client.assert_called_once_with(project=project, location=location)  # type: ignore


def test_normalize_string_value_strips_padding() -> None:
    assert _normalize_string_value("  29P  ") == "29P"
    assert _normalize_string_value("  071") == "071"


def test_sql_string_list_normalizes_and_escapes() -> None:
    values = [" 29P ", "O'Brien", "  071  "]
    assert _sql_string_list(values) == "'29P', 'O''Brien', '071'"


def test_query_observations_default_mode_is_minimal(mocker: MockFixture) -> None:
    mock_client = mocker.Mock(spec=bigquery.Client)
    mock_client.query.side_effect = RuntimeError("stop")
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    client = BigQueryMPCClient(dataset_id="test_dataset")
    with pytest.raises(RuntimeError, match="stop"):
        client.query_observations(provids=["2013 RR165"], limit=100)

    query = mock_client.query.call_args.args[0]
    assert "SELECT DISTINCT" in query
    assert "obs_sbn.obsid" in query
    assert "obs_sbn.obstime" in query
    assert "obs_sbn.ra" in query
    assert "obs_sbn.dec" in query
    assert "obs_sbn.mag" in query
    assert "obs_sbn.notes" not in query
    assert "obs_sbn.poscov11" not in query


def test_query_observations_ades_mode_includes_expanded_columns(mocker: MockFixture) -> None:
    mock_client = mocker.Mock(spec=bigquery.Client)
    mock_client.query.side_effect = RuntimeError("stop")
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    client = BigQueryMPCClient(dataset_id="test_dataset")
    with pytest.raises(RuntimeError, match="stop"):
        client.query_observations(
            provids=["2013 RR165"],
            column_mode="ades",
            dedupe=False,
            limit=100,
        )

    query = mock_client.query.call_args.args[0]
    assert "SELECT DISTINCT" not in query
    assert "SELECT" in query
    assert "obs_sbn.poscov11" in query
    assert "obs_sbn.notes" in query
    assert "obs_sbn.obstime_text" in query
