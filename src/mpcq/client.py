from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Literal, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from adam_core.observations import ADESObservations
from adam_core.time import Timestamp
from astropy.time import Time
from google.cloud import bigquery

from .observations import CrossMatchedMPCObservations, MPCObservations
from .orbits import MPCOrbits, MPCPrimaryObjects
from .submissions import (
    MPCSubmissionHistory,
    MPCSubmissionResults,
    infer_submission_time,
)

METERS_PER_ARCSECONDS = 30.87


def _iso_utc(col: pa.ChunkedArray) -> list[str]:
    """Convert a timestamp column to ISO-8601 UTC strings.

    BigQuery returns TIMESTAMP as Arrow timestamp[us, tz=UTC]. Casting to string yields
    'YYYY-MM-DD HH:MM:SS.ffffffZ'. Replace the space with 'T' for ISO-8601.
    """
    arr = pc.replace_substring(col.cast(pa.string()), " ", "T").combine_chunks()
    return arr.to_pylist()


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
    columns: list[str] | str | None, all_columns: Iterable[str], required: Iterable[str]
) -> list[str]:
    if columns is None:
        # Default to complete
        selected = list(all_columns)
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
            params.append(bigquery.ScalarQueryParameter(pname, None, f.value))
            clauses.append(f"{col} {f.op} @{pname}")
            param_index += 1
        elif op == "between":
            if not isinstance(f.value, tuple) or len(f.value) != 2:
                raise ValueError("between requires a (low, high) tuple")
            pname1 = f"{param_prefix}{param_index}"
            pname2 = f"{param_prefix}{param_index + 1}"
            params.append(bigquery.ScalarQueryParameter(pname1, None, f.value[0]))
            params.append(bigquery.ScalarQueryParameter(pname2, None, f.value[1]))
            clauses.append(f"{col} BETWEEN @{pname1} AND @{pname2}")
            param_index += 2
        elif op == "in":
            if not isinstance(f.value, (list, tuple)):
                raise ValueError("in requires a list/tuple value")
            pname = f"{param_prefix}{param_index}"
            params.append(bigquery.ArrayQueryParameter(pname, "STRING", list(f.value)))
            clauses.append(f"{col} IN UNNEST(@{pname})")
            param_index += 1
        elif op in {"is null", "is not null"}:
            clauses.append(f"{col} {op.upper()}")
        elif op in {"startswith", "endswith", "contains"}:
            if f.value is None:
                raise ValueError(f"Operator {f.op} requires a non-null value for {col}")
            params.append(bigquery.ScalarQueryParameter(pname, None, f.value))
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
            params.append(bigquery.ScalarQueryParameter(pname, None, str(f.value).lower()))
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


class MPCClient(ABC):
    @abstractmethod
    def query_observations(
        self,
        provids: list[str] | None = None,
        columns: list[str] | str | None = "*",
        where: list[Where] | None = None,
        limit: int | None = None,
    ) -> MPCObservations:
        """
        Query the MPC database for the observations and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str] | None
            List of provisional designations to query. Optional.
        columns : list[str] | str | None
            Select subset of columns or "*" (default) for all.
        where : list[Where] | None
            Additional filters using allowed operators.
        limit : int | None
            Limit the number of rows returned. Required if both provids and where are None.

        Returns
        -------
        observations : MPCObservations
            The observations and associated data for the given provisional designations.
        """
        pass

    @abstractmethod
    def query_orbits(self, provids: list[str]) -> MPCOrbits:
        """
        Query the MPC database for the orbits and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str]
            List of provisional designations to query.

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
        views_dataset_id: str,
        **kwargs: Any,
    ) -> None:
        self.client = bigquery.Client(**kwargs)
        self.dataset_id = dataset_id
        self.views_dataset_id = views_dataset_id

    def query_observations(
        self,
        provids: list[str] | None = None,
        columns: list[str] | str | None = "*",
        where: list[Where] | None = None,
        limit: int | None = None,
    ) -> MPCObservations:
        """
        Query the MPC database for the observations and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str] | None
            List of provisional designations to query. Optional.
        columns : list[str] | str | None
            Select subset of columns or "*" (default) for all.
        where : list[Where] | None
            Additional filters using allowed operators.
        limit : int | None
            Limit the number of rows returned. Required if both provids and where are None.

        Returns
        -------
        observations : MPCObservations
            The observations and associated data for the given provisional designations.
        """
        # Validation for no filters
        if provids is None and where is None and limit is None:
            raise ValueError("limit is required when neither provids nor where are provided")

        # Determine valid/required columns
        all_columns = set(MPCObservations.schema.names)
        # Exclude nested columns in this table definition
        required_cols = ["requested_provid", "primary_designation"]
        selected_cols = _normalize_columns(columns, all_columns, required_cols)

        # Build SELECT list, prepend metadata
        select_list = []
        if "requested_provid" in selected_cols:
            select_list.append("rp.provid AS requested_provid")
        if "primary_designation" in selected_cols:
            select_list.append(
                "CASE WHEN ni.permid IS NOT NULL THEN ni.permid ELSE ci.unpacked_primary_provisional_designation END AS primary_designation"
            )
        # Map remaining columns from obs_sbn
        for col in selected_cols:
            if col in {"requested_provid", "primary_designation"}:
                continue
            select_list.append(f"obs_sbn.{col}")

        select_sql = ",\n            ".join(select_list)

        # Build optional WHERE
        where_sql, params = _build_where_clause(where, set(MPCObservations.schema.names), "p_")

        # Build base query parts
        with_requested = ""
        join_condition = ""
        order_by = "ORDER BY obs_sbn.obstime ASC"

        if provids is not None:
            provids_str = ", ".join([f'"{id}"' for id in provids])
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
        LEFT JOIN `{self.dataset_id}.public_obs_sbn` AS obs_sbn
            ON ci.unpacked_primary_provisional_designation = obs_sbn.provid
            OR ci_alt.unpacked_secondary_provisional_designation = obs_sbn.provid
            OR ni.permid = obs_sbn.permid
            """
            order_by = "ORDER BY requested_provid ASC, obs_sbn.obstime ASC"
        else:
            # No provids; join only the main table, fabricate requested_provid and primary_designation if asked
            join_condition = f"""
        FROM `{self.dataset_id}.public_obs_sbn` AS obs_sbn
            """
            if "requested_provid" in selected_cols:
                select_list[0] = "obs_sbn.provid AS requested_provid"
            if "primary_designation" in selected_cols:
                select_list[1] = "obs_sbn.provid AS primary_designation"
            select_sql = ",\n            ".join(select_list)

        limit_sql = f"LIMIT {int(limit)}" if limit is not None else ""

        query = f"""
        {with_requested}
        SELECT DISTINCT
            {select_sql}
        {join_condition}
        {where_sql}
        {order_by}
        {limit_sql};
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        results = self.client.query(query, job_config=job_config).result()
        table = results.to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)

        obstime_iso = _iso_utc(table["obstime"]) if "obstime" in table.column_names else None
        created_at_iso = _iso_utc(table["created_at"]) if "created_at" in table.column_names else None
        updated_at_iso = _iso_utc(table["updated_at"]) if "updated_at" in table.column_names else None

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

        created_at_iso = _iso_utc(table["created_at"]) if "created_at" in table.column_names else None
        updated_at_iso = _iso_utc(table["updated_at"]) if "updated_at" in table.column_names else None
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
            created_at=Timestamp.from_iso8601(created_at_iso, scale="utc") if created_at_iso is not None else None,
            updated_at=Timestamp.from_iso8601(updated_at_iso, scale="utc") if updated_at_iso is not None else None,
        )

    def query_orbits(
        self,
        provids: list[str] | None = None,
        columns: list[str] | str | None = "*",
        where: list[Where] | None = None,
        limit: int | None = None,
    ) -> MPCOrbits:
        """
        Query the MPC database for the orbits and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str] | None
            List of provisional designations to query. Optional.
        columns : list[str] | str | None
            Select subset of columns or "*" (default) for all.
        where : list[Where] | None
            Additional filters using allowed operators.
        limit : int | None
            Limit the number of rows returned. Required if both provids and where are None.

        Returns
        -------
        orbits : MPCOrbits
            The orbits and associated data for the given provisional designations.
        """
        if provids is None and where is None and limit is None:
            raise ValueError("limit is required when neither provids nor where are provided")

        all_columns = set(MPCOrbits.schema.names)
        required_cols = ["requested_provid", "primary_designation", "provid", "epoch"]
        selected_cols = _normalize_columns(columns, all_columns, required_cols)

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
            select_list.append(f"mpc_orbits.{col}")

        select_sql = ",\n            ".join(select_list)

        where_sql, params = _build_where_clause(where, set(MPCOrbits.schema.names), "o_")

        with_requested = ""
        join_condition = ""
        order_by = "ORDER BY mpc_orbits.epoch_mjd ASC"

        if provids is not None:
            provids_str = ", ".join([f'"{id}"' for id in provids])
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

        query = f"""
        {with_requested}
        SELECT DISTINCT 
            {select_sql}
        {join_condition}
        {where_sql}
        {order_by}
        {limit_sql};
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        results = self.client.query(query, job_config=job_config).result()
        table = results.to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)

        created_at_iso = _iso_utc(table["created_at"]) 
        updated_at_iso = _iso_utc(table["updated_at"]) 
        fitting_datetime_iso = _iso_utc(table["fitting_datetime"]) if "fitting_datetime" in table.column_names else None

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
                        kwargs[name] = Timestamp.from_iso8601(created_at_iso, scale="utc")
                    elif name == "updated_at":
                        kwargs[name] = Timestamp.from_iso8601(updated_at_iso, scale="utc")
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
        submission_ids_str = ", ".join([f'"{id}"' for id in submission_ids])
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
        provids_str = ", ".join([f'"{id}"' for id in provids])
        query = f"""
        WITH requested_provids AS (
            SELECT provid
            FROM UNNEST(ARRAY[{provids_str}]) AS provid
        )
        SELECT DISTINCT
            rp.provid AS requested_provid,
            CASE 
                WHEN ni.permid IS NOT NULL THEN ni.permid 
                ELSE ci.unpacked_primary_provisional_designation
            END AS primary_designation,
            obs_sbn.obsid, 
            obs_sbn.obstime,
            obs_sbn.submission_id
        FROM requested_provids AS rp 
        LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci
            ON ci.unpacked_secondary_provisional_designation = rp.provid
        LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci_alt
            ON ci.unpacked_primary_provisional_designation = ci_alt.unpacked_primary_provisional_designation
        LEFT JOIN `{self.dataset_id}.public_numbered_identifications` AS ni
            ON ci.unpacked_primary_provisional_designation = ni.unpacked_primary_provisional_designation
        LEFT JOIN `{self.dataset_id}.public_obs_sbn` AS obs_sbn
            ON ci.unpacked_primary_provisional_designation = obs_sbn.provid
            OR ci_alt.unpacked_secondary_provisional_designation = obs_sbn.provid
            OR ni.permid = obs_sbn.permid
        ORDER BY requested_provid ASC, obs_sbn.obstime ASC;
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
        provids_str = ", ".join([f'"{id}"' for id in provids])

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

        # Create the STRUCT entries for each observation
        struct_entries = []
        for obsSubID, obsTime, ra, dec, stn in zip(
            ades_observations.obsSubID.to_numpy(zero_copy_only=False),
            ades_observations.obsTime.to_astropy().isot,
            ades_observations.ra.to_numpy(zero_copy_only=False),
            ades_observations.dec.to_numpy(zero_copy_only=False),
            ades_observations.stn.to_numpy(zero_copy_only=False),
        ):
            struct_entries.append(
                f"STRUCT('{obsSubID}' AS id, '{stn}' AS stn, {ra} AS ra, {dec} AS dec, "
                f"TIMESTAMP('{obsTime}') AS obstime)"
            )

        struct_str = ",\n        ".join(struct_entries)

        # First query to get matches using materialized view
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
        )
        SELECT 
            input.id AS input_id,
            clustered.id AS obs_id,
            ST_DISTANCE(clustered.st_geo, input.input_geo) AS separation_meters,
            TIMESTAMP_DIFF(clustered.obstime, input.obstime, SECOND) AS separation_seconds
        FROM input_observations AS input
        JOIN `{self.views_dataset_id}.public_obs_sbn_clustered` AS clustered
            ON clustered.stn = input.stn
            AND clustered.obstime BETWEEN 
                TIMESTAMP_SUB(input.obstime, INTERVAL {obstime_tolerance_seconds} SECOND)
                AND TIMESTAMP_ADD(input.obstime, INTERVAL {obstime_tolerance_seconds} SECOND)
            AND ST_DISTANCE(clustered.st_geo, input.input_geo) <= {meters_tolerance}
        """

        # Get the matched IDs using PyArrow
        matched_results = (
            self.client.query(matching_query)
            .result()
            .to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)
        )

        if len(matched_results) == 0:
            return CrossMatchedMPCObservations.empty()

        # Create a query to get the full data using the matched IDs
        matched_structs = ",".join(
            [
                f"STRUCT('{input_id}' as input_id, {obs_id} as obs_id, {separation_meters} as separation_meters, {separation_seconds} as separation_seconds)"
                for input_id, obs_id, separation_meters, separation_seconds in zip(
                    matched_results["input_id"].to_numpy(zero_copy_only=False),
                    matched_results["obs_id"].to_numpy(zero_copy_only=False),
                    matched_results["separation_meters"].to_numpy(zero_copy_only=False),
                    matched_results["separation_seconds"].to_numpy(zero_copy_only=False),
                )
            ]
        )

        final_query = f"""
        WITH matches AS (
            SELECT * FROM UNNEST([
                {matched_structs}
            ])
        )
        SELECT 
            m.input_id,
            m.separation_meters,
            m.separation_seconds,
            obs.*
        FROM matches m
        JOIN `{self.dataset_id}.public_obs_sbn` obs
            ON obs.id = m.obs_id
        ORDER BY m.input_id, m.separation_meters, m.separation_seconds
        """

        # Get final results as PyArrow table
        table = (
            self.client.query(final_query)
            .result()
            .to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)
        ).combine_chunks()

        obstime_iso = _iso_utc(table["obstime"])
        created_at_iso = _iso_utc(table["created_at"]) 
        updated_at_iso = _iso_utc(table["updated_at"]) 

        separation_arcseconds = (
            table["separation_meters"].to_numpy(zero_copy_only=False) * METERS_PER_ARCSECONDS
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
                ST_GEOGPOINT(CAST(ra AS FLOAT64), CAST(dec AS FLOAT64)) AS geo
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
            AND b.obstime BETWEEN 
                TIMESTAMP_SUB(a.obstime, INTERVAL {obstime_tolerance_seconds} SECOND)
                AND TIMESTAMP_ADD(a.obstime, INTERVAL {obstime_tolerance_seconds} SECOND)
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
