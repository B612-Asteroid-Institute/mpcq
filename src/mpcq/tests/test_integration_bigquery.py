import os
import pytest
from adam_core.observations import ADESObservations
from adam_core.time import Timestamp
from astropy.time import Time

from mpcq.client import BigQueryMPCClient, Where


pytestmark = pytest.mark.integration


RUN_IT = os.getenv("MPCQ_RUN_IT") == "1"
if not RUN_IT:
    pytest.skip("Integration tests disabled: set MPCQ_RUN_IT=1 to enable", allow_module_level=True)


DATASET = os.getenv("MPCQ_IT_DATASET", "moeyens-thor-dev.mpcq_it")
VIEWS_DATASET = os.getenv("MPCQ_IT_VIEWS_DATASET", "moeyens-thor-dev.mpcq_it_views")
MAX_BYTES = int(os.getenv("MPCQ_IT_MAX_BYTES", "2000000000"))
BASE_PROVIDS = [p.strip() for p in os.getenv("MPCQ_IT_BASE_PROVIDS", "2025 MW47,2025 PR1,1948 AD,1999 XK100").split(",")]


@pytest.fixture(scope="session")
def client() -> BigQueryMPCClient:
    c = BigQueryMPCClient(dataset_id=DATASET, views_dataset_id=VIEWS_DATASET)

    # Wrap query() to enforce per-job bytes cap and caching
    original_query = c.client.query

    def _query(q, job_config=None):  # type: ignore[no-untyped-def]
        from google.cloud import bigquery

        job_config = job_config or bigquery.QueryJobConfig()
        # Respect existing params while enforcing max bytes and cache
        job_config.maximum_bytes_billed = getattr(job_config, "maximum_bytes_billed", None) or MAX_BYTES
        job_config.use_query_cache = True
        return original_query(q, job_config=job_config)

    c.client.query = _query  # type: ignore[assignment]
    return c


def test_query_observations_default_all_columns(client: BigQueryMPCClient) -> None:
    res: MPCObservations = client.query_observations(provids=BASE_PROVIDS, limit=5000)
    assert len(res) > 0
    # Spot-check a few expected columns
    expected = {"requested_provid", "primary_designation", "provid", "obstime", "stn"}
    assert expected.issubset(set(res.table.column_names))


def test_query_observations_with_filters_and_subset_columns(client: BigQueryMPCClient) -> None:
    # Case-insensitive filter on station and time range
    where = [
        Where(column="stn", op="istartswith", value="t"),
        Where(
            column="obstime",
            op="between",
            value=(Time("2015-01-01T00:00:00Z"), Time("2035-01-01T00:00:00Z")),
        ),
    ]
    res = client.query_observations(provids=None, where=where, columns=["obsid", "stn", "obstime"], limit=100)
    assert len(res) > 0
    cols = set(res.table.column_names)
    # Required metadata is included by the client
    assert {"requested_provid", "primary_designation", "obsid", "stn", "obstime"}.issubset(cols)


def test_provids_none_requires_limit(client: BigQueryMPCClient) -> None:
    with pytest.raises(ValueError):
        client.query_observations(provids=None, where=None, limit=None)


def test_query_orbits_and_conversion(client: BigQueryMPCClient) -> None:
    orbits = client.query_orbits(provids=BASE_PROVIDS, limit=100)
    assert len(orbits) > 0
    # Required columns present
    assert {"provid", "epoch", "q", "e", "i"}.issubset(set(orbits.table.column_names))
    # Conversion to adam_core.Orbits should succeed for populated rows
    _ = orbits.orbits()


def test_query_primary_objects(client: BigQueryMPCClient) -> None:
    res = client.query_primary_objects(provids=BASE_PROVIDS)
    assert len(res) > 0
    assert {"provid", "created_at", "updated_at"}.issubset(set(res.table.column_names))


def test_cross_match_smoke(client: BigQueryMPCClient) -> None:
    # Build one synthetic ADES observation near a known observation
    from google.cloud import bigquery

    q = f"""
    SELECT id, stn, obstime, CAST(ra AS FLOAT64) ra, CAST(dec AS FLOAT64) dec
    FROM `{DATASET}.public_obs_sbn`
    WHERE stn IS NOT NULL AND obstime IS NOT NULL AND ra IS NOT NULL AND dec IS NOT NULL
    ORDER BY obstime DESC
    LIMIT 1
    """
    row = next(client.client.query(q).result())

    # Normalize to ISO without timezone suffix for astropy 'isot'
    obstime_iso = row["obstime"].isoformat().replace(" ", "T").split("+")[0]
    obstime = Time([obstime_iso], format="isot", scale="utc")
    ades = ADESObservations.from_kwargs(
        obsTime=Timestamp.from_astropy(obstime),
        ra=[float(row["ra"])],
        dec=[float(row["dec"])],
        stn=[row["stn"]],
        obsSubID=["IT-1"],
        mode=["VIS"],
        astCat=["Gaia"],
    )
    res = client.cross_match_observations(ades)
    assert len(res) >= 0  # smoke: runs and returns a table


