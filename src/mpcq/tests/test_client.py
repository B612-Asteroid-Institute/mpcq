import pytest
from google.cloud import bigquery
from pytest_mock import MockFixture

from mpcq.client import BigQueryMPCClient


def test_client_initialization(mocker: MockFixture) -> None:
    # Create mock client
    mock_client = mocker.Mock(spec=bigquery.Client)
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    # Test initialization with required dataset IDs
    dataset_id = "test_dataset"
    views_dataset_id = "test_views_dataset"
    client = BigQueryMPCClient(
        dataset_id=dataset_id,
        views_dataset_id=views_dataset_id,
    )

    assert client.dataset_id == dataset_id
    assert client.views_dataset_id == views_dataset_id


def test_client_initialization_missing_dataset_id() -> None:
    # Test initialization without required dataset_id
    with pytest.raises(TypeError, match=r".*missing.*required.*argument.*dataset_id"):
        BigQueryMPCClient(views_dataset_id="test_views_dataset")  # type: ignore


def test_client_initialization_missing_views_dataset_id() -> None:
    # Test initialization without required views_dataset_id
    with pytest.raises(
        TypeError, match=r".*missing.*required.*argument.*views_dataset_id"
    ):
        BigQueryMPCClient(dataset_id="test_dataset")  # type: ignore


def test_client_initialization_with_kwargs(mocker: MockFixture) -> None:
    # Create mock client
    mock_client = mocker.Mock(spec=bigquery.Client)
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    # Test initialization with additional kwargs
    dataset_id = "test_dataset"
    views_dataset_id = "test_views_dataset"
    project = "test_project"
    location = "test_location"

    client = BigQueryMPCClient(
        dataset_id=dataset_id,
        views_dataset_id=views_dataset_id,
        project=project,
        location=location,
    )

    assert client.dataset_id == dataset_id
    assert client.views_dataset_id == views_dataset_id

    # Verify that kwargs were passed to BigQuery client
    bigquery.Client.assert_called_once_with(project=project, location=location)  # type: ignore
