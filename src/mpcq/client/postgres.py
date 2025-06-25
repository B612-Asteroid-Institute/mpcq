from typing import Any, List

import adbc_driver_postgresql
import adbc_driver_postgresql.dbapi
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from adam_core.observations import ADESObservations
from adam_core.time import Timestamp
from astropy.time import Time

from ..observations import CrossMatchedMPCObservations, MPCObservations
from ..orbits import MPCOrbits, MPCPrimaryObjects
from ..submissions import (
    MPCSubmissionHistory,
    MPCSubmissionResults,
    infer_submission_time,
)
from .client import MPCClient


def _to_postgres_string_array(values: List[str]) -> str:
    return ", ".join([f"'{value}'" for value in values])


class PostgresMPCClient(MPCClient):

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5432,
        schema: str = "public",
        **kwargs: Any,
    ) -> None:
        """
        Initialize a PostgreSQL client for MPC database access using ADBC.

        Parameters
        ----------
        database : str
            Name of the database to connect to
        user : str
            Username for database authentication
        host : str
            Database server hostname
        port : int
            Database server port
        password : str
            Password for database authentication
        schema : str, optional
            Database schema to use, defaults to "public"
        **kwargs : Any
            Additional keyword arguments passed to connection
        """
        self.connection_params = {
            "database": database,
            "user": user,
            "host": host,
            "port": str(port),
            "password": password,
            **kwargs,
        }
        self.schema = schema
        self._conn = None

    @property
    def conn(self) -> adbc_driver_postgresql.dbapi.Connection:
        if self._conn is None:
            # Create a PostgreSQL connection string
            conn_string = (
                f"dbname={self.connection_params['database']} "
                f"user={self.connection_params['user']} "
                f"password={self.connection_params['password']} "
                f"host={self.connection_params['host']} "
                f"port={self.connection_params['port']}"
            )
            self._conn = adbc_driver_postgresql.dbapi.connect(uri=conn_string)
        return self._conn

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __del__(self) -> None:
        self.close()

    def query_observations(self, provids: List[str]) -> MPCObservations:
        """
        Query the MPC database for the observations and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str]
            List of provisional designations to query.

        Returns
        -------
        observations : MPCObservations
            The observations and associated data for the given provisional designations.
        """
        query = f"""
        WITH requested_provids AS (
            SELECT UNNEST(ARRAY[{_to_postgres_string_array(provids)}]) AS provid
        )
        SELECT DISTINCT
            rp.provid AS requested_provid,
            CASE 
                WHEN ni.permid IS NOT NULL THEN ni.permid 
                ELSE ci.unpacked_primary_provisional_designation
            END AS primary_designation,
            obs_sbn.obsid, 
            obs_sbn.trksub, 
            obs_sbn.permid, 
            obs_sbn.provid, 
            obs_sbn.submission_id, 
            obs_sbn.obssubid, 
            obs_sbn.obstime, 
            obs_sbn.ra, 
            obs_sbn.dec, 
            obs_sbn.rmsra, 
            obs_sbn.rmsdec, 
            obs_sbn.rmscorr,
            obs_sbn.mag, 
            obs_sbn.rmsmag, 
            obs_sbn.band, 
            obs_sbn.stn, 
            obs_sbn.updated_at, 
            obs_sbn.created_at, 
            obs_sbn.status,
            obs_sbn.astcat,
            obs_sbn.mode
        FROM requested_provids AS rp
        LEFT JOIN current_identifications AS ci
            ON ci.unpacked_secondary_provisional_designation = rp.provid
        LEFT JOIN current_identifications AS ci_alt
            ON ci.unpacked_primary_provisional_designation = ci_alt.unpacked_primary_provisional_designation
        LEFT JOIN numbered_identifications AS ni
            ON ci.unpacked_primary_provisional_designation = ni.unpacked_primary_provisional_designation
        LEFT JOIN obs_sbn AS obs_sbn
            ON ci.unpacked_primary_provisional_designation = obs_sbn.provid
            OR ci_alt.unpacked_secondary_provisional_designation = obs_sbn.provid
            OR ni.permid = obs_sbn.permid
        ORDER BY requested_provid ASC, obs_sbn.obstime ASC
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            table = cursor.fetch_arrow_table().combine_chunks()

        if len(table) == 0:
            return MPCObservations.empty()

        obstime = Time(
            table.column("obstime").to_pylist(), format="datetime", scale="utc"
        )
        created_at = Time(
            table.column("created_at").to_pylist(), format="datetime", scale="utc"
        )
        updated_at = Time(
            table.column("updated_at").to_pylist(), format="datetime", scale="utc"
        )

        return MPCObservations.from_kwargs(
            requested_provid=pc.cast(
                table.column("requested_provid"), pa.large_string()
            ),
            obsid=pc.cast(table.column("obsid"), pa.large_string()),
            primary_designation=pc.cast(
                table.column("primary_designation"), pa.large_string()
            ),
            trksub=pc.cast(table.column("trksub"), pa.large_string()),
            provid=pc.cast(table.column("provid"), pa.large_string()),
            permid=pc.cast(table.column("permid"), pa.large_string()),
            submission_id=pc.cast(table.column("submission_id"), pa.large_string()),
            obssubid=pc.cast(table.column("obssubid"), pa.large_string()),
            obstime=Timestamp.from_astropy(obstime),
            ra=pc.cast(table.column("ra"), pa.float64()),
            dec=pc.cast(table.column("dec"), pa.float64()),
            rmsra=pc.cast(table.column("rmsra"), pa.float64()),
            rmsdec=pc.cast(table.column("rmsdec"), pa.float64()),
            rmscorr=pc.cast(table.column("rmscorr"), pa.float64()),
            mag=pc.cast(table.column("mag"), pa.float64()),
            rmsmag=pc.cast(table.column("rmsmag"), pa.float64()),
            band=pc.cast(table.column("band"), pa.large_string()),
            stn=pc.cast(table.column("stn"), pa.large_string()),
            updated_at=Timestamp.from_astropy(updated_at),
            created_at=Timestamp.from_astropy(created_at),
            status=pc.cast(table.column("status"), pa.large_string()),
            astcat=pc.cast(table.column("astcat"), pa.large_string()),
            mode=pc.cast(table.column("mode"), pa.large_string()),
        )

    def query_orbits(self, provids: List[str]) -> MPCOrbits:
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
        query = f"""
        WITH requested_provids AS (
            SELECT provid
            FROM UNNEST(ARRAY[{_to_postgres_string_array(provids)}]) AS provid
        )
        SELECT DISTINCT 
            rp.provid AS requested_provid,
            CASE
                WHEN ni.permid IS NOT NULL THEN ni.permid
                ELSE ci.unpacked_primary_provisional_designation
            END AS primary_designation,
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
        FROM requested_provids AS rp
        LEFT JOIN current_identifications AS ci
            ON ci.unpacked_secondary_provisional_designation = rp.provid
        LEFT JOIN current_identifications AS ci_alt
            ON ci.unpacked_primary_provisional_designation = ci_alt.unpacked_primary_provisional_designation
        LEFT JOIN numbered_identifications AS ni
            ON ci.unpacked_primary_provisional_designation = ni.unpacked_primary_provisional_designation
        LEFT JOIN mpc_orbits AS mpc_orbits
            ON ci.unpacked_primary_provisional_designation = mpc_orbits.unpacked_primary_provisional_designation
        ORDER BY 
            requested_provid ASC,
            mpc_orbits.epoch_mjd ASC
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            table = cursor.fetch_arrow_table().combine_chunks()

        created_at = Time(
            table.column("created_at").to_pylist(),
            format="datetime",
            scale="utc",
        )
        updated_at = Time(
            table.column("updated_at").to_pylist(),
            format="datetime",
            scale="utc",
        )

        # Handle NULL values in the epoch_mjd column: ideally
        # we should have the Timestamp class be able to handle this
        mjd_array = table.column("epoch_mjd").to_pylist()
        mjds = np.ma.masked_array(mjd_array, mask=np.isnan(mjd_array))  # type: ignore
        epoch = Time(mjds, format="mjd", scale="tt")

        return MPCOrbits.from_kwargs(
            requested_provid=pc.cast(
                table.column("requested_provid"), pa.large_string()
            ),
            primary_designation=pc.cast(
                table.column("primary_designation"), pa.large_string()
            ),
            id=pc.cast(table.column("id"), pa.int64()),
            provid=pc.cast(table.column("provid"), pa.large_string()),
            epoch=Timestamp.from_astropy(epoch),
            q=pc.cast(table.column("q"), pa.float64()),
            e=pc.cast(table.column("e"), pa.float64()),
            i=pc.cast(table.column("i"), pa.float64()),
            node=pc.cast(table.column("node"), pa.float64()),
            argperi=pc.cast(table.column("argperi"), pa.float64()),
            peri_time=pc.cast(table.column("peri_time"), pa.float64()),
            q_unc=pc.cast(table.column("q_unc"), pa.float64()),
            e_unc=pc.cast(table.column("e_unc"), pa.float64()),
            i_unc=pc.cast(table.column("i_unc"), pa.float64()),
            node_unc=pc.cast(table.column("node_unc"), pa.float64()),
            argperi_unc=pc.cast(table.column("argperi_unc"), pa.float64()),
            peri_time_unc=pc.cast(table.column("peri_time_unc"), pa.float64()),
            a1=pc.cast(table.column("a1"), pa.float64()),
            a2=pc.cast(table.column("a2"), pa.float64()),
            a3=pc.cast(table.column("a3"), pa.float64()),
            h=pc.cast(table.column("h"), pa.float64()),
            g=pc.cast(table.column("g"), pa.float64()),
            created_at=Timestamp.from_astropy(created_at),
            updated_at=Timestamp.from_astropy(updated_at),
        )

    def query_submission_results(
        self, submission_ids: List[str]
    ) -> MPCSubmissionResults:
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
        query = f"""
        WITH requested_submission_ids AS (
            SELECT submission_id
            FROM UNNEST(ARRAY[{_to_postgres_string_array(submission_ids)}]) AS submission_id
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
        LEFT JOIN obs_sbn AS obs_sbn
            ON sb.submission_id = obs_sbn.submission_id
        LEFT JOIN current_identifications AS ci
            ON ci.unpacked_secondary_provisional_designation = obs_sbn.provid
            OR ci.unpacked_primary_provisional_designation = obs_sbn.provid
        LEFT JOIN numbered_identifications AS ni
            ON obs_sbn.permid = ni.permid
        ORDER BY requested_submission_id ASC, obs_sbn.obsid ASC
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            table = cursor.fetch_arrow_table().combine_chunks()

        return MPCSubmissionResults.from_kwargs(
            requested_submission_id=pc.cast(
                table.column("requested_submission_id"), pa.large_string()
            ),
            obsid=pc.cast(table.column("obsid"), pa.large_string()),
            obssubid=pc.cast(table.column("obssubid"), pa.large_string()),
            trksub=pc.cast(table.column("trksub"), pa.large_string()),
            primary_designation=pc.cast(
                table.column("primary_designation"), pa.large_string()
            ),
            provid=pc.cast(table.column("provid"), pa.large_string()),
            submission_id=pc.cast(table.column("submission_id"), pa.large_string()),
            status=pc.cast(table.column("status"), pa.large_string()),
        )

    def query_submission_history(self, provids: List[str]) -> MPCSubmissionHistory:
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
        query = f"""
        WITH requested_provids AS (
            SELECT provid
            FROM UNNEST(ARRAY[{_to_postgres_string_array(provids)}]) AS provid
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
        LEFT JOIN current_identifications AS ci
            ON ci.unpacked_secondary_provisional_designation = rp.provid
        LEFT JOIN current_identifications AS ci_alt
            ON ci.unpacked_primary_provisional_designation = ci_alt.unpacked_primary_provisional_designation
        LEFT JOIN numbered_identifications AS ni
            ON ci.unpacked_primary_provisional_designation = ni.unpacked_primary_provisional_designation
        LEFT JOIN obs_sbn AS obs_sbn
            ON ci.unpacked_primary_provisional_designation = obs_sbn.provid
            OR ci_alt.unpacked_secondary_provisional_designation = obs_sbn.provid
            OR ni.permid = obs_sbn.permid
        ORDER BY requested_provid ASC, obs_sbn.obstime ASC
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            table = cursor.fetch_arrow_table().combine_chunks()

        table = (
            table.group_by(["requested_provid", "primary_designation", "submission_id"])
            .aggregate(
                [("obsid", "count_distinct"), ("obstime", "min"), ("obstime", "max")]
            )
            .sort_by(
                [("primary_designation", "ascending"), ("submission_id", "ascending")]
            )
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
        first_last_idx = table.group_by(
            ["primary_designation"], use_threads=False
        ).aggregate([("idx", "first"), ("idx", "last")])
        first = np.zeros(len(table), dtype=bool)
        last = np.zeros(len(table), dtype=bool)
        first[first_last_idx["idx_first"].to_numpy(zero_copy_only=False)] = True
        last[first_last_idx["idx_last"].to_numpy(zero_copy_only=False)] = True
        table = table.append_column("first_submission", pa.array(first))
        table = table.append_column("last_submission", pa.array(last))

        # Calculate the arc length of each submission
        start_times = Time(
            table["first_obs_time"].to_numpy(zero_copy_only=False), scale="utc"
        )
        end_times = Time(
            table["last_obs_time"].to_numpy(zero_copy_only=False), scale="utc"
        )
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
            first_obs_time=Timestamp.from_astropy(start_times),
            last_obs_time=Timestamp.from_astropy(end_times),
            arc_length=arc_length,
        ).sort_by(["requested_provid", "submission_id"])

    def query_primary_objects(self, provids: List[str]) -> MPCPrimaryObjects:
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
        query = f"""
        WITH requested_provids AS (
            SELECT provid
            FROM UNNEST(ARRAY[{_to_postgres_string_array(provids)}]) AS provid
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
        LEFT JOIN current_identifications AS ci
            ON ci.unpacked_secondary_provisional_designation = rp.provid
        LEFT JOIN current_identifications AS ci_alt
            ON ci.unpacked_primary_provisional_designation = ci_alt.unpacked_primary_provisional_designation
        LEFT JOIN numbered_identifications AS ni
            ON ci.unpacked_primary_provisional_designation = ni.unpacked_primary_provisional_designation
        LEFT JOIN primary_objects AS po
            ON ci.unpacked_primary_provisional_designation = po.unpacked_primary_provisional_designation
        ORDER BY requested_provid ASC
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            table = cursor.fetch_arrow_table().combine_chunks()

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
            created_at=Timestamp.from_astropy(created_at),
            updated_at=Timestamp.from_astropy(updated_at),
        )

    def cross_match_observations(
        self,
        ades_observations: ADESObservations,
        obstime_tolerance_seconds: int = 30,
        arcseconds_tolerance: float = 2.0,
    ) -> CrossMatchedMPCObservations:
        pass

    def find_duplicates(
        self,
        provid: str,
        obstime_tolerance_seconds: int = 30,
        arcseconds_tolerance: float = 2.0,
    ) -> CrossMatchedMPCObservations:
        pass

    def query_submission_num_obs(self, submission_id: str) -> int:
        pass
