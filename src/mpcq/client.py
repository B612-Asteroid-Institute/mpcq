from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Literal, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from adam_core.observations import ADESObservations
from adam_core.time import Timestamp
from astropy.time import Time, TimeDelta
from google.cloud import bigquery

from .observations import CrossMatchedMPCObservations, MPCObservations
from .orbits import MPCOrbits, MPCPrimaryObjects
from .submissions import (
    MPCSubmissionHistory,
    MPCSubmissionResults,
    infer_submission_time,
)

METERS_PER_ARCSECONDS = 30.87
MAX_CROSSMATCH_INPUT_ROWS_PER_QUERY = 500
ObservationColumnMode = Literal["minimal", "ades", "full"]
OrbitColumnMode = Literal["minimal", "full"]

# Small default payload intended for most query/list use cases.
OBSERVATION_COLUMNS_MINIMAL = [
    "obsid",
    "provid",
    "permid",
    "obstime",
    "ra",
    "dec",
    "stn",
    "mag",
    "band",
    "status",
]

# ADES-compatible payload including expanded ADES fields.
OBSERVATION_COLUMNS_ADES = [
    "obsid",
    "obssubid",
    "trksub",
    "trkid",
    "provid",
    "permid",
    "submission_id",
    "submission_block_id",
    "obs80",
    "status",
    "ref",
    "mode",
    "stn",
    "trx",
    "rcv",
    "sys",
    "ctr",
    "pos1",
    "pos2",
    "pos3",
    "poscov11",
    "poscov12",
    "poscov13",
    "poscov22",
    "poscov23",
    "poscov33",
    "prog",
    "obstime",
    "ra",
    "dec",
    "rastar",
    "decstar",
    "obscenter",
    "deltara",
    "deltadec",
    "dist",
    "pa",
    "rmsra",
    "rmsdec",
    "rmsdist",
    "rmspa",
    "rmscorr",
    "delay",
    "rmsdelay",
    "doppler",
    "rmsdoppler",
    "astcat",
    "mag",
    "rmsmag",
    "band",
    "photcat",
    "photap",
    "nucmag",
    "logsnr",
    "seeing",
    "exp",
    "rmsfit",
    "com",
    "frq",
    "disc",
    "subfrm",
    "subfmt",
    "prectime",
    "precra",
    "precdec",
    "unctime",
    "notes",
    "remarks",
    "deprecated",
    "localuse",
    "nstars",
    "prev_desig",
    "prev_ref",
    "rmstime",
    "trkmpc",
    "designation_asterisk",
    "obstime_text",
]

ORBIT_COLUMNS_MINIMAL = [
    "provid",
    "epoch",
    "q",
    "e",
    "i",
    "node",
    "argperi",
    "peri_time",
    "h",
    "g",
]


def _iso_utc(col: pa.ChunkedArray) -> list[str]:
    """Convert a timestamp column to ISO-8601 UTC strings.

    BigQuery returns TIMESTAMP as Arrow timestamp[us, tz=UTC]. Casting to string yields
    'YYYY-MM-DD HH:MM:SS.ffffffZ'. Replace the space with 'T' for ISO-8601.
    """
    arr = pc.replace_substring(col.cast(pa.string()), " ", "T").combine_chunks()
    return arr.to_pylist()


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _normalize_string_value(value: Any) -> str:
    return str(value).strip()


def _sql_string_list(values: Sequence[Any]) -> str:
    return ", ".join(
        [f"'{_escape_sql_string(_normalize_string_value(value))}'" for value in values]
    )


@dataclass(frozen=True)
class Where:
    column: str
    op: Literal[
        "=",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
        "between",
        "in",
        "is null",
        "is not null",
        "startswith",
        "endswith",
        "contains",
        "istartswith",
        "iendswith",
        "icontains",
    ]
    value: Any | Sequence[Any] | tuple[Any, Any] | None = None


def _normalize_columns(
    columns: list[str] | str | None,
    mode_columns: list[str],
    all_columns: Iterable[str],
    required: Iterable[str],
) -> list[str]:
    if columns is None:
        selected = list(mode_columns)
    elif columns == "*":
        selected = list(all_columns)
    else:
        selected = list(columns)

    # Always include required metadata columns
    for col in required:
        if col not in selected:
            selected.append(col)
    return selected


def _build_where_clause(
    filters: list[Where] | None,
    valid_columns: set[str],
    param_prefix: str,
):
    if not filters:
        return "", []

    params: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] = []
    clauses: list[str] = []
    param_index = 0

    for f in filters:
        col = f.column
        if col not in valid_columns:
            raise ValueError(f"Invalid column in where: {col}")

        op = f.op.lower()
        pname = f"{param_prefix}{param_index}"

        if op in {"=", "!=", "<", "<=", ">", ">="}:
            if f.value is None:
                raise ValueError(f"Operator {f.op} requires a non-null value for {col}")
            ptype, pvalue = _to_bq_param(f.value)
            params.append(bigquery.ScalarQueryParameter(pname, ptype, pvalue))
            clauses.append(f"{col} {f.op} @{pname}")
            param_index += 1
        elif op == "between":
            if not isinstance(f.value, tuple) or len(f.value) != 2:
                raise ValueError("between requires a (low, high) tuple")
            pname1 = f"{param_prefix}{param_index}"
            pname2 = f"{param_prefix}{param_index + 1}"
            ptype1, pvalue1 = _to_bq_param(f.value[0])
            ptype2, pvalue2 = _to_bq_param(f.value[1])
            params.append(bigquery.ScalarQueryParameter(pname1, ptype1, pvalue1))
            params.append(bigquery.ScalarQueryParameter(pname2, ptype2, pvalue2))
            clauses.append(f"{col} BETWEEN @{pname1} AND @{pname2}")
            param_index += 2
        elif op == "in":
            if not isinstance(f.value, (list, tuple)):
                raise ValueError("in requires a list/tuple value")
            if len(f.value) == 0:
                raise ValueError("in requires a non-empty list/tuple value")
            pname = f"{param_prefix}{param_index}"
            array_type, _ = _to_bq_param(f.value[0])
            array_values = [_to_bq_param(v)[1] for v in f.value]
            params.append(bigquery.ArrayQueryParameter(pname, array_type, array_values))
            clauses.append(f"{col} IN UNNEST(@{pname})")
            param_index += 1
        elif op in {"is null", "is not null"}:
            clauses.append(f"{col} {op.upper()}")
        elif op in {"startswith", "endswith", "contains"}:
            if f.value is None:
                raise ValueError(f"Operator {f.op} requires a non-null value for {col}")
            params.append(bigquery.ScalarQueryParameter(pname, "STRING", str(f.value)))
            func = {
                "startswith": "STARTS_WITH",
                "endswith": "ENDS_WITH",
                "contains": "STRPOS",
            }[op]
            if func == "STRPOS":
                clauses.append(f"STRPOS(CAST({col} AS STRING), CAST(@{pname} AS STRING)) > 0")
            else:
                clauses.append(f"{func}(CAST({col} AS STRING), CAST(@{pname} AS STRING))")
            param_index += 1
        elif op in {"istartswith", "iendswith", "icontains"}:
            if f.value is None:
                raise ValueError(f"Operator {f.op} requires a non-null value for {col}")
            params.append(bigquery.ScalarQueryParameter(pname, "STRING", str(f.value).lower()))
            lowered = f"LOWER(CAST({col} AS STRING))"
            if op == "icontains":
                clauses.append(f"STRPOS({lowered}, CAST(@{pname} AS STRING)) > 0")
            elif op == "istartswith":
                clauses.append(f"STARTS_WITH({lowered}, CAST(@{pname} AS STRING))")
            else:
                clauses.append(f"ENDS_WITH({lowered}, CAST(@{pname} AS STRING))")
            param_index += 1
        else:
            raise ValueError(f"Unsupported operator: {f.op}")

    return ("WHERE " + " AND ".join(clauses), params)


def _to_bq_param(value: Any) -> tuple[str, Any]:
    if isinstance(value, Time):
        return "TIMESTAMP", value.to_datetime()
    if isinstance(value, datetime):
        return "TIMESTAMP", value
    if isinstance(value, date):
        return "DATE", value
    if isinstance(value, bool):
        return "BOOL", value
    if isinstance(value, int):
        return "INT64", value
    if isinstance(value, float):
        return "FLOAT64", value
    return "STRING", str(value)


class MPCClient(ABC):
    @abstractmethod
    def query_observations(
        self,
        provids: list[str] | None = None,
        columns: list[str] | str | None = None,
        column_mode: ObservationColumnMode = "minimal",
        where: list[Where] | None = None,
        limit: int | None = None,
        dedupe: bool = True,
    ) -> MPCObservations:
        """
        Query the MPC database for the observations and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str] | None
            List of provisional designations to query. Optional.
        columns : list[str] | str | None
            Explicit subset of columns, "*" for full schema, or None to use column_mode.
        column_mode : Literal["minimal", "ades", "full"]
            Default column bundle when columns is None.
        where : list[Where] | None
            Additional filters using allowed operators.
        limit : int | None
            Limit the number of rows returned. Required if both provids and where are None.
        dedupe : bool
            If True, use SELECT DISTINCT to deduplicate expanded-identifier joins.

        Returns
        -------
        observations : MPCObservations
            The observations and associated data for the given provisional designations.
        """
        pass

    @abstractmethod
    def query_orbits(
        self,
        provids: list[str] | None = None,
        columns: list[str] | str | None = None,
        column_mode: OrbitColumnMode = "minimal",
        where: list[Where] | None = None,
        limit: int | None = None,
        dedupe: bool = True,
    ) -> MPCOrbits:
        """
        Query the MPC database for the orbits and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str] | None
            List of provisional designations to query. Optional.
        columns : list[str] | str | None
            Explicit subset of columns, "*" for full schema, or None to use column_mode.
        column_mode : Literal["minimal", "full"]
            Default column bundle when columns is None.
        where : list[Where] | None
            Additional filters using allowed operators.
        limit : int | None
            Limit the number of rows returned. Required if both provids and where are None.
        dedupe : bool
            If True, use SELECT DISTINCT to deduplicate expanded-identifier joins.

        Returns
        -------
        orbits : MPCOrbits
            The orbits and associated data for the given provisional designations.
        """
        pass

    @abstractmethod
    def query_submission_info(self, submission_ids: list[str]) -> MPCSubmissionResults:
        """
        Query for observation status and mapping (observation ID to trksub, provid, etc.) for a
        given list of submission IDs.

        Parameters
        ----------
        submission_ids : List[str]
            List of submission IDs to query.

        Returns
        -------
        submission_info : MPCSubmissionResults
            The observation status and mapping for the given submission IDs.
        """
        pass

    @abstractmethod
    def query_submission_history(self, provids: list[str]) -> MPCSubmissionHistory:
        """
        Query for submission history for a given list of provisional designations.

        Parameters
        ----------
        provids : List[str]
            List of provisional designations to query.

        Returns
        -------
        submission_history : MPCSubmissionHistory
            The submission history for the given provisional designations.
        """
        pass

    @abstractmethod
    def query_primary_objects(self, provids: list[str]) -> MPCPrimaryObjects:
        """
        Query the MPC database for the primary objects and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str]
            List of provisional designations to query.

        Returns
        -------
        primary_objects : MPCPrimaryObjects
            The primary objects and associated data for the given provisional designations.
        """
        pass

    @abstractmethod
    def cross_match_observations(
        self,
        ades_observations: ADESObservations,
        obstime_tolerance_seconds: int = 30,
        arcseconds_tolerance: float = 2.0,
    ) -> CrossMatchedMPCObservations:
        """
        Cross-match the given ADES observations with the MPC observations.

        Parameters
        ----------
        ades_observations : ADESObservations
            The ADES observations to cross-match.
        obstime_tolerance_seconds : int, optional
            Time tolerance in seconds for matching observations.
        arcseconds_tolerance : float, optional
            Angular separation tolerance in arcseconds.

        Returns
        -------
        cross_matched_mpc_observations : CrossMatchedMPCObservations
            The MPC observations that match the given ADES observations.
        """
        pass

    @abstractmethod
    def find_duplicates(
        self,
        provid: str,
        obstime_tolerance_seconds: int = 30,
        arcseconds_tolerance: float = 2.0,
    ) -> CrossMatchedMPCObservations:
        """
        Find duplicates in the MPC observations for a given object by comparing
        observations against each other using time and position tolerances.

        Parameters
        ----------
        provid : str
            The provisional designation to check for duplicates.
        obstime_tolerance_seconds : int, optional
            Time tolerance in seconds for matching observations.
        arcseconds_tolerance : float, optional
            Angular separation tolerance in arcseconds.

        Returns
        -------
        cross_matched_mpc_observations : CrossMatchedMPCObservations
            The MPC observations that are potential duplicates, with separation
            information included.
        """
        pass


class BigQueryMPCClient(MPCClient):
    def __init__(
        self,
        dataset_id: str,
        **kwargs: Any,
    ) -> None:
        self.client = bigquery.Client(**kwargs)
        self.dataset_id = dataset_id

    def query_observations(
        self,
        provids: list[str] | None = None,
        columns: list[str] | str | None = None,
        column_mode: ObservationColumnMode = "minimal",
        where: list[Where] | None = None,
        limit: int | None = None,
        dedupe: bool = True,
    ) -> MPCObservations:
        """
        Query the MPC database for the observations and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str] | None
            List of provisional designations to query. Optional.
        columns : list[str] | str | None
            Explicit subset of columns, "*" for full schema, or None to use column_mode.
        column_mode : Literal["minimal", "ades", "full"]
            Default column bundle when columns is None.
        where : list[Where] | None
            Additional filters using allowed operators.
        limit : int | None
            Limit the number of rows returned. Required if both provids and where are None.
        dedupe : bool
            If True, use SELECT DISTINCT to deduplicate expanded-identifier joins.

        Returns
        -------
        observations : MPCObservations
            The observations and associated data for the given provisional designations.
        """
        # Validation for no filters
        if provids is None and where is None and limit is None:
            raise ValueError("limit is required when neither provids nor where are provided")

        all_columns = list(MPCObservations.schema.names)
        all_column_set = set(all_columns)
        required_cols = ["requested_provid", "primary_designation"]
        mode_map: dict[ObservationColumnMode, list[str]] = {
            "minimal": [c for c in OBSERVATION_COLUMNS_MINIMAL if c in all_column_set],
            "ades": [c for c in OBSERVATION_COLUMNS_ADES if c in all_column_set],
            "full": list(all_columns),
        }
        if column_mode not in mode_map:
            raise ValueError(f"Unsupported observation column_mode: {column_mode}")
        selected_cols = _normalize_columns(
            columns, mode_map[column_mode], all_columns, required_cols
        )

        # Build optional WHERE
        where_sql, params = _build_where_clause(where, all_column_set, "p_")

        # Build base query parts
        with_clause = ""
        from_clause = ""
        order_by = "ORDER BY obs_sbn.obstime ASC"

        if provids is not None:
            provids_str = _sql_string_list(provids)
            with_clause = f"""
        WITH requested_provids AS (
            SELECT provid
            FROM UNNEST(ARRAY[{provids_str}]) AS provid
        ),
        requested_identifiers AS (
            SELECT
                rp.provid AS requested_provid,
                CASE
                    WHEN ni.permid IS NOT NULL THEN ni.permid
                    ELSE ci.unpacked_primary_provisional_designation
                END AS primary_designation,
                ci.unpacked_primary_provisional_designation AS primary_provid,
                ci_alt.unpacked_secondary_provisional_designation AS secondary_provid,
                ni.permid AS numbered_permid
            FROM requested_provids AS rp
            LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci
                ON ci.unpacked_secondary_provisional_designation = rp.provid
            LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci_alt
                ON ci.unpacked_primary_provisional_designation = ci_alt.unpacked_primary_provisional_designation
            LEFT JOIN `{self.dataset_id}.public_numbered_identifications` AS ni
                ON ci.unpacked_primary_provisional_designation = ni.unpacked_primary_provisional_designation
        ),
        candidate_matches AS (
            SELECT
                ri.requested_provid,
                ri.primary_designation,
                obs_sbn.*
            FROM requested_identifiers AS ri
            JOIN `{self.dataset_id}.public_obs_sbn` AS obs_sbn
                ON obs_sbn.provid = ri.primary_provid

            UNION ALL

            SELECT
                ri.requested_provid,
                ri.primary_designation,
                obs_sbn.*
            FROM requested_identifiers AS ri
            JOIN `{self.dataset_id}.public_obs_sbn` AS obs_sbn
                ON obs_sbn.provid = ri.secondary_provid

            UNION ALL

            SELECT
                ri.requested_provid,
                ri.primary_designation,
                obs_sbn.*
            FROM requested_identifiers AS ri
            JOIN `{self.dataset_id}.public_obs_sbn` AS obs_sbn
                ON obs_sbn.permid = ri.numbered_permid
        )
            """
            from_clause = "FROM candidate_matches AS obs_sbn"
            order_by = "ORDER BY requested_provid ASC, obs_sbn.obstime ASC"
        else:
            with_clause = f"""
        WITH candidate_matches AS (
            SELECT
                obs_sbn.provid AS requested_provid,
                obs_sbn.provid AS primary_designation,
                obs_sbn.*
            FROM `{self.dataset_id}.public_obs_sbn` AS obs_sbn
        )
            """
            from_clause = "FROM candidate_matches AS obs_sbn"

        # Build SELECT list, prepend metadata
        select_list = []
        if "requested_provid" in selected_cols:
            select_list.append("obs_sbn.requested_provid AS requested_provid")
        if "primary_designation" in selected_cols:
            select_list.append("obs_sbn.primary_designation AS primary_designation")
        for col in selected_cols:
            if col in {"requested_provid", "primary_designation"}:
                continue
            if col in {"all_pub_ref", "datastream_metadata"}:
                select_list.append(f"TO_JSON_STRING(obs_sbn.{col}) AS {col}")
            else:
                select_list.append(f"obs_sbn.{col}")

        select_sql = ",\n            ".join(select_list)

        limit_sql = f"LIMIT {int(limit)}" if limit is not None else ""
        select_keyword = "SELECT DISTINCT" if dedupe else "SELECT"

        query = f"""
        {with_clause}
        {select_keyword}
            {select_sql}
        {from_clause}
        {where_sql}
        {order_by}
        {limit_sql};
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        results = self.client.query(query, job_config=job_config).result()
        table = results.to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)

        obstime_iso = _iso_utc(table["obstime"]) if "obstime" in table.column_names else None
        created_at_iso = (
            _iso_utc(table["created_at"]) if "created_at" in table.column_names else None
        )
        updated_at_iso = (
            _iso_utc(table["updated_at"]) if "updated_at" in table.column_names else None
        )

        # Ensure time-like columns cast when present; fill missing required schema columns
        kwargs: dict[str, Any] = {}
        for name in MPCObservations.schema.names:
            if name in table.column_names:
                if name in {"obstime", "created_at", "updated_at"}:
                    if name == "obstime" and obstime_iso is not None:
                        val = Timestamp.from_iso8601(obstime_iso, scale="utc")
                    elif name == "created_at" and created_at_iso is not None:
                        val = Timestamp.from_iso8601(created_at_iso, scale="utc")
                    elif name == "updated_at" and updated_at_iso is not None:
                        val = Timestamp.from_iso8601(updated_at_iso, scale="utc")
                    else:
                        continue
                else:
                    val = table[name]
                kwargs[name] = val

        return MPCObservations.from_kwargs(**kwargs)

    def all_orbits(self) -> MPCOrbits:
        """
        Query the MPC database for all orbits and associated data.

        Returns
        -------
        orbits : MPCOrbits
            The orbits and associated data for all objects in the MPC database.
        """
        query = f"""
        SELECT
            mpc_orbits.id, 
            mpc_orbits.unpacked_primary_provisional_designation AS provid, 
            mpc_orbits.epoch_mjd,
            mpc_orbits.q, 
            mpc_orbits.e,
            mpc_orbits.i, 
            mpc_orbits.node,
            mpc_orbits.argperi,
            mpc_orbits.peri_time,
            mpc_orbits.q_unc,
            mpc_orbits.e_unc,
            mpc_orbits.i_unc,
            mpc_orbits.node_unc,
            mpc_orbits.argperi_unc,
            mpc_orbits.peri_time_unc,
            mpc_orbits.a1,
            mpc_orbits.a2,
            mpc_orbits.a3,
            mpc_orbits.h,
            mpc_orbits.g,
            mpc_orbits.created_at,
            mpc_orbits.updated_at
        FROM `{self.dataset_id}.public_mpc_orbits` AS mpc_orbits
        ORDER BY mpc_orbits.epoch_mjd ASC;
        """
        query_job = self.client.query(query)
        results = query_job.result()

        table = results.to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)

        created_at_iso = (
            _iso_utc(table["created_at"]) if "created_at" in table.column_names else None
        )
        updated_at_iso = (
            _iso_utc(table["updated_at"]) if "updated_at" in table.column_names else None
        )
        epoch_ts = Timestamp.from_mjd(table["epoch_mjd"], scale="tt")

        return MPCOrbits.from_kwargs(
            # Note, since we didn't request a specific provid we use the one MPC provides
            requested_provid=table["provid"],
            id=table["id"],
            provid=table["provid"],
            epoch=epoch_ts,
            q=table["q"],
            e=table["e"],
            i=table["i"],
            node=table["node"],
            argperi=table["argperi"],
            peri_time=table["peri_time"],
            q_unc=table["q_unc"],
            e_unc=table["e_unc"],
            i_unc=table["i_unc"],
            node_unc=table["node_unc"],
            argperi_unc=table["argperi_unc"],
            peri_time_unc=table["peri_time_unc"],
            a1=table["a1"],
            a2=table["a2"],
            a3=table["a3"],
            h=table["h"],
            g=table["g"],
            created_at=Timestamp.from_iso8601(created_at_iso, scale="utc")
            if created_at_iso is not None
            else None,
            updated_at=Timestamp.from_iso8601(updated_at_iso, scale="utc")
            if updated_at_iso is not None
            else None,
        )

    def query_orbits(
        self,
        provids: list[str] | None = None,
        columns: list[str] | str | None = None,
        column_mode: OrbitColumnMode = "minimal",
        where: list[Where] | None = None,
        limit: int | None = None,
        dedupe: bool = True,
    ) -> MPCOrbits:
        """
        Query the MPC database for the orbits and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str] | None
            List of provisional designations to query. Optional.
        columns : list[str] | str | None
            Explicit subset of columns, "*" for full schema, or None to use column_mode.
        column_mode : Literal["minimal", "full"]
            Default column bundle when columns is None.
        where : list[Where] | None
            Additional filters using allowed operators.
        limit : int | None
            Limit the number of rows returned. Required if both provids and where are None.
        dedupe : bool
            If True, use SELECT DISTINCT to deduplicate expanded-identifier joins.

        Returns
        -------
        orbits : MPCOrbits
            The orbits and associated data for the given provisional designations.
        """
        if provids is None and where is None and limit is None:
            raise ValueError("limit is required when neither provids nor where are provided")

        all_columns = list(MPCOrbits.schema.names)
        all_column_set = set(all_columns)
        required_cols = ["requested_provid", "primary_designation", "provid", "epoch"]
        mode_map: dict[OrbitColumnMode, list[str]] = {
            "minimal": [c for c in ORBIT_COLUMNS_MINIMAL if c in all_column_set],
            "full": list(all_columns),
        }
        if column_mode not in mode_map:
            raise ValueError(f"Unsupported orbit column_mode: {column_mode}")
        selected_cols = _normalize_columns(
            columns, mode_map[column_mode], all_columns, required_cols
        )

        select_list = []
        if "requested_provid" in selected_cols:
            select_list.append("rp.provid AS requested_provid")
        if "primary_designation" in selected_cols:
            select_list.append(
                "CASE WHEN ni.permid IS NOT NULL THEN ni.permid ELSE ci.unpacked_primary_provisional_designation END AS primary_designation"
            )
        if "provid" in selected_cols:
            select_list.append("mpc_orbits.unpacked_primary_provisional_designation AS provid")
        if "epoch" in selected_cols:
            select_list.append("mpc_orbits.epoch_mjd")

        # Remaining columns
        for col in selected_cols:
            if col in {"requested_provid", "primary_designation", "provid", "epoch"}:
                continue
            if col in {"mpc_orb_jsonb", "datastream_metadata"}:
                select_list.append(f"TO_JSON_STRING(mpc_orbits.{col}) AS {col}")
            else:
                select_list.append(f"mpc_orbits.{col}")

        select_sql = ",\n            ".join(select_list)

        where_sql, params = _build_where_clause(where, all_column_set, "o_")

        with_requested = ""
        join_condition = ""
        order_by = "ORDER BY mpc_orbits.epoch_mjd ASC"

        if provids is not None:
            provids_str = _sql_string_list(provids)
            with_requested = f"""
        WITH requested_provids AS (
            SELECT provid
            FROM UNNEST(ARRAY[{provids_str}]) AS provid
        )
            """
            join_condition = f"""
        FROM requested_provids AS rp
        LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci
            ON ci.unpacked_secondary_provisional_designation = rp.provid
        LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci_alt
            ON ci.unpacked_primary_provisional_designation = ci_alt.unpacked_primary_provisional_designation
        LEFT JOIN `{self.dataset_id}.public_numbered_identifications` AS ni
            ON ci.unpacked_primary_provisional_designation = ni.unpacked_primary_provisional_designation
        LEFT JOIN `{self.dataset_id}.public_mpc_orbits` AS mpc_orbits
            ON ci.unpacked_primary_provisional_designation = mpc_orbits.unpacked_primary_provisional_designation
            """
            order_by = "ORDER BY requested_provid ASC, mpc_orbits.epoch_mjd ASC"
        else:
            join_condition = f"""
        FROM `{self.dataset_id}.public_mpc_orbits` AS mpc_orbits
            """
            if "requested_provid" in selected_cols:
                select_list[0] = (
                    "mpc_orbits.unpacked_primary_provisional_designation AS requested_provid"
                )
            if "primary_designation" in selected_cols:
                idx = 1 if "requested_provid" in selected_cols else 0
                select_list[idx] = (
                    "mpc_orbits.unpacked_primary_provisional_designation AS primary_designation"
                )
            select_sql = ",\n            ".join(select_list)

        limit_sql = f"LIMIT {int(limit)}" if limit is not None else ""
        select_keyword = "SELECT DISTINCT" if dedupe else "SELECT"

        query = f"""
        {with_requested}
        {select_keyword}
            {select_sql}
        {join_condition}
        {where_sql}
        {order_by}
        {limit_sql};
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        results = self.client.query(query, job_config=job_config).result()
        table = results.to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)

        created_at_iso = (
            _iso_utc(table["created_at"]) if "created_at" in table.column_names else None
        )
        updated_at_iso = (
            _iso_utc(table["updated_at"]) if "updated_at" in table.column_names else None
        )
        fitting_datetime_iso = (
            _iso_utc(table["fitting_datetime"])
            if "fitting_datetime" in table.column_names
            else None
        )

        # Handle NULL values in the epoch_mjd column: ideally
        # we should have the Timestamp class be able to handle this
        mjd_array = table["epoch_mjd"].to_numpy(zero_copy_only=False)
        mjds = np.ma.masked_array(mjd_array, mask=np.isnan(mjd_array))  # type: ignore
        epoch = Time(mjds, format="mjd", scale="tt")

        # Build kwargs dynamically
        kwargs: dict[str, Any] = {}
        for name in MPCOrbits.schema.names:
            if name in table.column_names:
                if name == "epoch":
                    kwargs[name] = Timestamp.from_iso8601(epoch.isot, scale="tt")
                elif name in {"created_at", "updated_at", "fitting_datetime"}:
                    if name == "created_at":
                        if created_at_iso is not None:
                            kwargs[name] = Timestamp.from_iso8601(created_at_iso, scale="utc")
                        else:
                            continue
                    elif name == "updated_at":
                        if updated_at_iso is not None:
                            kwargs[name] = Timestamp.from_iso8601(updated_at_iso, scale="utc")
                        else:
                            continue
                    else:
                        if fitting_datetime_iso is not None:
                            kwargs[name] = Timestamp.from_iso8601(fitting_datetime_iso, scale="utc")
                        else:
                            continue
                else:
                    kwargs[name] = table[name]

        # Always ensure provid and epoch are present if requested
        if (
            "provid" in MPCOrbits.schema.names
            and "provid" not in kwargs
            and "provid" in table.column_names
        ):
            kwargs["provid"] = table["provid"]
        if "epoch" in MPCOrbits.schema.names and "epoch" not in kwargs:
            kwargs["epoch"] = Timestamp.from_iso8601(epoch.isot, scale="tt")

        return MPCOrbits.from_kwargs(**kwargs)

    def query_submission_info(self, submission_ids: list[str]) -> MPCSubmissionResults:
        """
        Query for observation status and mapping (observation ID to trksub, provid, etc.)
        for a given list of submission IDs.

        Parameters
        ----------
        submission_ids : list[str]
            List of submission IDs to query.

        Returns
        -------
        submission_info : MPCSubmissionResults
            The observation status and mapping for the given submission IDs.
        """
        submission_ids_str = _sql_string_list(submission_ids)
        query = f"""
        WITH requested_submission_ids AS (
            SELECT submission_id
            FROM UNNEST(ARRAY[{submission_ids_str}]) AS submission_id
        )
        SELECT DISTINCT
            sb.submission_id AS requested_submission_id,
            obs_sbn.obsid,
            obs_sbn.obssubid, 
            obs_sbn.trksub, 
            CASE 
                WHEN ni.permid IS NOT NULL THEN ni.permid 
                ELSE ci.unpacked_primary_provisional_designation
            END AS primary_designation,
            obs_sbn.permid, 
            obs_sbn.provid, 
            obs_sbn.submission_id, 
            obs_sbn.status
        FROM requested_submission_ids AS sb
        LEFT JOIN `{self.dataset_id}.public_obs_sbn` AS obs_sbn
            ON sb.submission_id = obs_sbn.submission_id
        LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci
            ON ci.unpacked_secondary_provisional_designation = obs_sbn.provid
            OR ci.unpacked_primary_provisional_designation = obs_sbn.provid
        LEFT JOIN `{self.dataset_id}.public_numbered_identifications` AS ni
            ON obs_sbn.permid = ni.permid
        ORDER BY requested_submission_id ASC, obs_sbn.obsid ASC;
        """
        query_job = self.client.query(query)
        results = query_job.result()
        table = results.to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)

        return MPCSubmissionResults.from_pyarrow(table)

    def query_submission_history(self, provids: list[str]) -> MPCSubmissionHistory:
        """
        Query for submission history for a given list of provisional designations.

        Parameters
        ----------
        provids : list[str]
            List of provisional designations to query.

        Returns
        -------
        submission_history : MPCSubmissionHistory
            The submission history for the given provisional designations.
        """
        provids_str = _sql_string_list(provids)
        query = f"""
        WITH requested_provids AS (
            SELECT provid
            FROM UNNEST(ARRAY[{provids_str}]) AS provid
        ),
        requested_identifiers AS (
            SELECT
                rp.provid AS requested_provid,
                CASE
                    WHEN ni.permid IS NOT NULL THEN ni.permid
                    ELSE ci.unpacked_primary_provisional_designation
                END AS primary_designation,
                ci.unpacked_primary_provisional_designation AS primary_provid,
                ci_alt.unpacked_secondary_provisional_designation AS secondary_provid,
                ni.permid AS numbered_permid
            FROM requested_provids AS rp
            LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci
                ON ci.unpacked_secondary_provisional_designation = rp.provid
            LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci_alt
                ON ci.unpacked_primary_provisional_designation = ci_alt.unpacked_primary_provisional_designation
            LEFT JOIN `{self.dataset_id}.public_numbered_identifications` AS ni
                ON ci.unpacked_primary_provisional_designation = ni.unpacked_primary_provisional_designation
        ),
        candidate_obs AS (
            SELECT
                ri.requested_provid,
                ri.primary_designation,
                obs_sbn.obsid,
                obs_sbn.obstime,
                obs_sbn.submission_id
            FROM requested_identifiers AS ri
            JOIN `{self.dataset_id}.public_obs_sbn` AS obs_sbn
                ON obs_sbn.provid = ri.primary_provid

            UNION ALL

            SELECT
                ri.requested_provid,
                ri.primary_designation,
                obs_sbn.obsid,
                obs_sbn.obstime,
                obs_sbn.submission_id
            FROM requested_identifiers AS ri
            JOIN `{self.dataset_id}.public_obs_sbn` AS obs_sbn
                ON obs_sbn.provid = ri.secondary_provid

            UNION ALL

            SELECT
                ri.requested_provid,
                ri.primary_designation,
                obs_sbn.obsid,
                obs_sbn.obstime,
                obs_sbn.submission_id
            FROM requested_identifiers AS ri
            JOIN `{self.dataset_id}.public_obs_sbn` AS obs_sbn
                ON obs_sbn.permid = ri.numbered_permid
        )
        SELECT DISTINCT
            requested_provid,
            primary_designation,
            obsid,
            obstime,
            submission_id
        FROM candidate_obs
        ORDER BY requested_provid ASC, obstime ASC;
        """
        query_job = self.client.query(query)
        results = query_job.result()

        # Convert the results to a PyArrow table
        table = results.to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)
        table = (
            table.group_by(["requested_provid", "primary_designation", "submission_id"])
            .aggregate([("obsid", "count_distinct"), ("obstime", "min"), ("obstime", "max")])
            .sort_by([("primary_designation", "ascending"), ("submission_id", "ascending")])
            .rename_columns(
                [
                    "requested_provid",
                    "primary_designation",
                    "submission_id",
                    "num_obs",
                    "first_obs_time",
                    "last_obs_time",
                ]
            )
        )

        # Create array that tracks the index of each row
        table = table.append_column("idx", pa.array(np.arange(len(table))))

        # Find the first and last index of each group (first and last submission)
        # and append boolean columns to the table
        first_last_idx = table.group_by(["primary_designation"], use_threads=False).aggregate(
            [("idx", "first"), ("idx", "last")]
        )
        first = np.zeros(len(table), dtype=bool)
        last = np.zeros(len(table), dtype=bool)
        first[first_last_idx["idx_first"].to_numpy(zero_copy_only=False)] = True
        last[first_last_idx["idx_last"].to_numpy(zero_copy_only=False)] = True
        table = table.append_column("first_submission", pa.array(first))
        table = table.append_column("last_submission", pa.array(last))

        # Calculate the arc length of each submission
        start_times = Time(table["first_obs_time"].to_numpy(zero_copy_only=False), scale="utc")
        end_times = Time(table["last_obs_time"].to_numpy(zero_copy_only=False), scale="utc")
        arc_length = end_times.utc.mjd - start_times.utc.mjd

        return MPCSubmissionHistory.from_kwargs(
            requested_provid=table["requested_provid"],
            primary_designation=table["primary_designation"],
            submission_id=table["submission_id"],
            submission_time=infer_submission_time(
                table["submission_id"].to_numpy(zero_copy_only=False),
                end_times.utc.isot,
            ),
            first_submission=table["first_submission"],
            last_submission=table["last_submission"],
            num_obs=table["num_obs"],
            first_obs_time=Timestamp.from_iso8601(start_times.utc.isot, scale="utc"),
            last_obs_time=Timestamp.from_iso8601(end_times.utc.isot, scale="utc"),
            arc_length=arc_length,
        )

    def query_primary_objects(self, provids: list[str]) -> MPCPrimaryObjects:
        """
        Query the MPC database for the primary objects and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : list[str]
            List of provisional designations to query.

        Returns
        -------
        primary_objects : MPCPrimaryObjects
            The primary objects and associated data for the given provisional designations.
        """
        provids_str = _sql_string_list(provids)

        query = f"""WITH requested_provids AS (
            SELECT provid
            FROM UNNEST(ARRAY[{provids_str}]) AS provid
        )
        SELECT DISTINCT
            rp.provid AS requested_provid,
            CASE 
                WHEN ni.permid IS NOT NULL THEN ni.permid 
                ELSE ci.unpacked_primary_provisional_designation
            END AS primary_designation,
            po.unpacked_primary_provisional_designation as provid, 
            po.created_at, 
            po.updated_at
        FROM requested_provids AS rp
        LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci
            ON ci.unpacked_secondary_provisional_designation = rp.provid
        LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci_alt
            ON ci.unpacked_primary_provisional_designation = ci_alt.unpacked_primary_provisional_designation
        LEFT JOIN `{self.dataset_id}.public_numbered_identifications` AS ni
            ON ci.unpacked_primary_provisional_designation = ni.unpacked_primary_provisional_designation
        LEFT JOIN `{self.dataset_id}.public_primary_objects` AS po
            ON ci.unpacked_primary_provisional_designation = po.unpacked_primary_provisional_designation
        ORDER BY requested_provid ASC;
        """
        query_job = self.client.query(query)
        results = query_job.result()
        table = results.to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)

        created_at = Time(
            table["created_at"].to_numpy(zero_copy_only=False),
            format="datetime64",
            scale="utc",
        )
        updated_at = Time(
            table["updated_at"].to_numpy(zero_copy_only=False),
            format="datetime64",
            scale="utc",
        )

        return MPCPrimaryObjects.from_kwargs(
            requested_provid=table["requested_provid"],
            primary_designation=table["primary_designation"],
            provid=table["provid"],
            created_at=Timestamp.from_iso8601(created_at.utc.isot, scale="utc"),
            updated_at=Timestamp.from_iso8601(updated_at.utc.isot, scale="utc"),
        )

    def cross_match_observations(
        self,
        ades_observations: ADESObservations,
        obstime_tolerance_seconds: int = 30,
        arcseconds_tolerance: float = 2.0,
    ) -> CrossMatchedMPCObservations:
        """
        Cross-match the given ADES observations with the MPC observations.

        Parameters
        ----------
        ades_observations : ADESObservations
            The ADES observations to cross-match.
        obstime_tolerance_seconds : float, optional
            Time tolerance in seconds for matching observations.
        arcseconds_tolerance : float, optional
            Angular separation tolerance in arcseconds.

        Returns
        -------
        cross_matched_mpc_observations : CrossMatchedMPCObservations
            The MPC observations that match the given ADES observations.
        """
        # We use the ADESObservation.obssubid as the unique identifier
        # to track the cross-match requests.
        assert pc.all(pc.invert(pc.is_null(ades_observations.obsSubID))).as_py()

        # Convert arcseconds to meters at Earth's surface (approximate)
        meters_tolerance = arcseconds_tolerance * METERS_PER_ARCSECONDS
        coarse_dec_tolerance_deg = arcseconds_tolerance / 3600.0

        input_rows = []
        for obsSubID, obsTime, ra, dec, stn in zip(
            ades_observations.obsSubID.to_numpy(zero_copy_only=False),
            ades_observations.obsTime.to_astropy(),
            ades_observations.ra.to_numpy(zero_copy_only=False),
            ades_observations.dec.to_numpy(zero_copy_only=False),
            ades_observations.stn.to_numpy(zero_copy_only=False),
        ):
            obstime_iso = obsTime.utc.isot
            input_rows.append(
                {
                    "id": _normalize_string_value(obsSubID),
                    "stn": _normalize_string_value(stn),
                    "ra": float(ra),
                    "dec": float(dec),
                    "obstime_iso": obstime_iso,
                    "month_bucket": obstime_iso[:7],
                }
            )

        if len(input_rows) == 0:
            return CrossMatchedMPCObservations.empty()

        # Keep bounds tight to preserve partition pruning and prevent a single
        # wide-spanning request from scanning large historical ranges.
        bucketed_rows = defaultdict(list)
        for row in input_rows:
            bucketed_rows[row["month_bucket"]].append(row)

        result_tables = []
        for month_key in sorted(bucketed_rows.keys()):
            month_rows = bucketed_rows[month_key]
            for start in range(0, len(month_rows), MAX_CROSSMATCH_INPUT_ROWS_PER_QUERY):
                batch_rows = month_rows[start : start + MAX_CROSSMATCH_INPUT_ROWS_PER_QUERY]
                min_obstime = min(row["obstime_iso"] for row in batch_rows)
                max_obstime = max(row["obstime_iso"] for row in batch_rows)
                min_bound = (
                    Time(min_obstime, format="isot", scale="utc")
                    - TimeDelta(obstime_tolerance_seconds, format="sec")
                ).utc.isot
                max_bound = (
                    Time(max_obstime, format="isot", scale="utc")
                    + TimeDelta(obstime_tolerance_seconds, format="sec")
                ).utc.isot
                station_literals = ", ".join(
                    [
                        f"'{_escape_sql_string(stn)}'"
                        for stn in sorted({row["stn"] for row in batch_rows})
                    ]
                )
                struct_entries = [
                    (
                        "STRUCT("
                        f"'{_escape_sql_string(row['id'])}' AS id, "
                        f"'{_escape_sql_string(row['stn'])}' AS stn, "
                        f"{row['ra']} AS ra, "
                        f"{row['dec']} AS dec, "
                        f"TIMESTAMP('{row['obstime_iso']}') AS obstime"
                        ")"
                    )
                    for row in batch_rows
                ]
                struct_str = ",\n                ".join(struct_entries)

                matching_query = f"""
                WITH input_observations AS (
                    SELECT
                        id,
                        stn,
                        ra,
                        dec,
                        obstime,
                        ST_GEOGPOINT(ra, dec) AS input_geo
                    FROM UNNEST([
                        {struct_str}
                    ])
                ),
                candidate_observations AS (
                    SELECT
                        obs.obsid,
                        obs.trksub,
                        obs.provid,
                        obs.permid,
                        obs.submission_id,
                        obs.obssubid,
                        obs.obstime,
                        obs.ra,
                        obs.dec,
                        obs.rmsra,
                        obs.rmsdec,
                        obs.mag,
                        obs.rmsmag,
                        obs.band,
                        obs.stn,
                        obs.updated_at,
                        obs.created_at,
                        obs.status,
                        SAFE_CAST(obs.ra AS FLOAT64) AS ra_f64,
                        SAFE_CAST(obs.dec AS FLOAT64) AS dec_f64,
                        ST_GEOGPOINT(SAFE_CAST(obs.ra AS FLOAT64), SAFE_CAST(obs.dec AS FLOAT64)) AS obs_geo
                    FROM `{self.dataset_id}.public_obs_sbn` AS obs
                    WHERE obs.stn IN ({station_literals})
                      AND obs.obstime BETWEEN TIMESTAMP('{min_bound}') AND TIMESTAMP('{max_bound}')
                )
                SELECT
                    input.id AS input_id,
                    ST_DISTANCE(obs.obs_geo, input.input_geo) AS separation_meters,
                    TIMESTAMP_DIFF(obs.obstime, input.obstime, SECOND) AS separation_seconds,
                    obs.obsid,
                    obs.trksub,
                    obs.provid,
                    obs.permid,
                    obs.submission_id,
                    obs.obssubid,
                    obs.obstime,
                    obs.ra,
                    obs.dec,
                    obs.rmsra,
                    obs.rmsdec,
                    obs.mag,
                    obs.rmsmag,
                    obs.band,
                    obs.stn,
                    obs.updated_at,
                    obs.created_at,
                    obs.status
                FROM input_observations AS input
                JOIN candidate_observations AS obs
                    ON obs.stn = input.stn
                    AND obs.obs_geo IS NOT NULL
                    AND obs.obstime BETWEEN
                        TIMESTAMP_SUB(input.obstime, INTERVAL {obstime_tolerance_seconds} SECOND)
                        AND TIMESTAMP_ADD(input.obstime, INTERVAL {obstime_tolerance_seconds} SECOND)
                    AND obs.dec_f64 BETWEEN input.dec - {coarse_dec_tolerance_deg} AND input.dec + {coarse_dec_tolerance_deg}
                    AND obs.ra_f64 BETWEEN
                        input.ra - ({coarse_dec_tolerance_deg} / GREATEST(0.1, COS(input.dec * ACOS(-1) / 180.0)))
                        AND input.ra + ({coarse_dec_tolerance_deg} / GREATEST(0.1, COS(input.dec * ACOS(-1) / 180.0)))
                WHERE ST_DISTANCE(obs.obs_geo, input.input_geo) <= {meters_tolerance}
                ORDER BY input_id, separation_meters, separation_seconds
                """

                result_table = (
                    self.client.query(matching_query)
                    .result()
                    .to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)
                )
                if len(result_table) > 0:
                    result_tables.append(result_table)

        if len(result_tables) == 0:
            return CrossMatchedMPCObservations.empty()

        table = pa.concat_tables(result_tables) if len(result_tables) > 1 else result_tables[0]
        table = table.combine_chunks()

        obstime_iso = _iso_utc(table["obstime"])
        created_at_iso = _iso_utc(table["created_at"])
        updated_at_iso = _iso_utc(table["updated_at"])

        separation_arcseconds = (
            table["separation_meters"].to_numpy(zero_copy_only=False) / METERS_PER_ARCSECONDS
        )

        return CrossMatchedMPCObservations.from_kwargs(
            request_id=table["input_id"],
            separation_arcseconds=separation_arcseconds,
            separation_seconds=table["separation_seconds"],
            mpc_observations=MPCObservations.from_kwargs(
                obsid=table["obsid"],
                trksub=table["trksub"],
                provid=table["provid"],
                permid=table["permid"],
                submission_id=table["submission_id"],
                obssubid=table["obssubid"],
                obstime=Timestamp.from_iso8601(obstime_iso, scale="utc"),
                ra=table["ra"],
                dec=table["dec"],
                rmsra=table["rmsra"],
                rmsdec=table["rmsdec"],
                mag=table["mag"],
                rmsmag=table["rmsmag"],
                band=table["band"],
                stn=table["stn"],
                updated_at=Timestamp.from_iso8601(updated_at_iso, scale="utc"),
                created_at=Timestamp.from_iso8601(created_at_iso, scale="utc"),
                status=table["status"],
            ),
        )

    def find_duplicates(
        self,
        provid: str,
        obstime_tolerance_seconds: int = 30,
        arcseconds_tolerance: float = 2.0,
    ) -> CrossMatchedMPCObservations:
        meters_tolerance = arcseconds_tolerance * METERS_PER_ARCSECONDS
        coarse_dec_tolerance_deg = arcseconds_tolerance / 3600.0
        provid = _escape_sql_string(_normalize_string_value(provid))

        query = f"""
        WITH obs AS (
            SELECT 
                obsid,
                stn,
                ra,
                dec,
                obstime,
                created_at,
                updated_at,
                trksub,
                provid,
                permid,
                submission_id,
                obssubid,
                rmsra,
                rmsdec,
                mag,
                rmsmag,
                band,
                status,
                SAFE_CAST(ra AS FLOAT64) AS ra_f64,
                SAFE_CAST(dec AS FLOAT64) AS dec_f64,
                ST_GEOGPOINT(SAFE_CAST(ra AS FLOAT64), SAFE_CAST(dec AS FLOAT64)) AS geo
            FROM `{self.dataset_id}.public_obs_sbn`
            WHERE provid = '{provid}'
        )
        SELECT 
            a.obsid AS input_id,
            b.obsid,
            ST_DISTANCE(a.geo, b.geo) AS separation_meters,
            TIMESTAMP_DIFF(b.obstime, a.obstime, SECOND) AS separation_seconds,
            b.trksub,
            b.provid,
            b.permid,
            b.submission_id,
            b.obssubid,
            b.obstime,
            b.ra,
            b.dec,
            b.rmsra,
            b.rmsdec,
            b.mag,
            b.rmsmag,
            b.band,
            b.stn,
            b.created_at,
            b.updated_at,
            b.status
        FROM obs a
        JOIN obs b
            ON b.stn = a.stn  -- Same station
            AND a.obsid < b.obsid  -- Avoid self-matches and duplicates
            AND a.geo IS NOT NULL
            AND b.geo IS NOT NULL
            AND b.obstime BETWEEN 
                TIMESTAMP_SUB(a.obstime, INTERVAL {obstime_tolerance_seconds} SECOND)
                AND TIMESTAMP_ADD(a.obstime, INTERVAL {obstime_tolerance_seconds} SECOND)
            AND b.dec_f64 BETWEEN a.dec_f64 - {coarse_dec_tolerance_deg} AND a.dec_f64 + {coarse_dec_tolerance_deg}
            AND b.ra_f64 BETWEEN
                a.ra_f64 - ({coarse_dec_tolerance_deg} / GREATEST(0.1, COS(a.dec_f64 * ACOS(-1) / 180.0)))
                AND a.ra_f64 + ({coarse_dec_tolerance_deg} / GREATEST(0.1, COS(a.dec_f64 * ACOS(-1) / 180.0)))
            AND ST_DISTANCE(a.geo, b.geo) <= {meters_tolerance}
        ORDER BY a.obsid, separation_meters
        """

        # Execute query and get results
        results = (
            self.client.query(query)
            .result()
            .to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)
        )

        if len(results) == 0:
            return CrossMatchedMPCObservations.empty()

        # Convert timestamps to ISO strings (no astropy)
        obstime_iso = pc.binary_join_element_wise(
            pc.replace_substring(results["obstime"].cast(pa.string()), " ", "T"),
            pa.scalar("Z"),
            pa.scalar(""),
        )
        created_at_iso = pc.binary_join_element_wise(
            pc.replace_substring(results["created_at"].cast(pa.string()), " ", "T"),
            pa.scalar("Z"),
            pa.scalar(""),
        )
        updated_at_iso = pc.binary_join_element_wise(
            pc.replace_substring(results["updated_at"].cast(pa.string()), " ", "T"),
            pa.scalar("Z"),
            pa.scalar(""),
        )

        # Convert meters to arcseconds
        separation_arcseconds = (
            results["separation_meters"].to_numpy(zero_copy_only=False) / METERS_PER_ARCSECONDS
        )

        return CrossMatchedMPCObservations.from_kwargs(
            request_id=results["input_id"].cast(pa.string()),
            separation_arcseconds=separation_arcseconds,
            separation_seconds=results["separation_seconds"],
            mpc_observations=MPCObservations.from_kwargs(
                obsid=results["obsid"],
                trksub=results["trksub"],
                provid=results["provid"],
                permid=results["permid"],
                submission_id=results["submission_id"],
                obssubid=results["obssubid"],
                obstime=Timestamp.from_iso8601(obstime_iso, scale="utc"),
                ra=results["ra"],
                dec=results["dec"],
                rmsra=results["rmsra"],
                rmsdec=results["rmsdec"],
                mag=results["mag"],
                rmsmag=results["rmsmag"],
                band=results["band"],
                stn=results["stn"],
                updated_at=Timestamp.from_iso8601(updated_at_iso, scale="utc"),
                created_at=Timestamp.from_iso8601(created_at_iso, scale="utc"),
                status=results["status"],
            ),
        )
