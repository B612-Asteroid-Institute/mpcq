from google.cloud import bigquery
from pytest_mock import MockFixture


def test_import_client() -> None:
    from mpcq.client import BigQueryMPCClient, MPCClient  # noqa: F401


def test_import_and_initialize_client(mocker: MockFixture) -> None:
    # Create mock client
    mock_client = mocker.Mock(spec=bigquery.Client)
    mocker.patch("google.cloud.bigquery.Client", return_value=mock_client)

    from mpcq.client import BigQueryMPCClient

    # Test initialization with required dataset ID
    dataset_id = "test_dataset"
    client = BigQueryMPCClient(dataset_id=dataset_id)

    assert isinstance(client, BigQueryMPCClient)
    assert client.dataset_id == dataset_id
