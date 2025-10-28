from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

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


class MPCClient(ABC):

    @abstractmethod
    def query_observations(
        self,
        provids: List[str],
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
    ) -> MPCObservations:
        """
        Query the MPC database for the observations and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str]
            List of provisional designations to query.
        include_columns : Optional[List[str]]
            Columns to include from `public_obs_sbn`. If None, includes a sensible default set.
            Use together with exclude_columns; include is applied first.
        exclude_columns : Optional[List[str]]
            Columns to exclude from the final selection. Applied after include_columns.

        Returns
        -------
        observations : MPCObservations
            The observations and associated data for the given provisional designations.
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def query_submission_info(self, submission_ids: List[str]) -> MPCSubmissionResults:
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
        pass

    @abstractmethod
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

    # Default columns if none specified
    default_columns: List[str] = [
        "id",
        "obsid",
        "trksub",
        "provid",
        "permid",
        "submission_id",
        "obssubid",
        "obstime",
        "ra",
        "dec",
        "rmsra",
        "rmsdec",
        "rmscorr",
        "mag",
        "rmsmag",
        "band",
        "stn",
        "updated_at",
        "created_at",
        "status",
        "astcat",
        "mode",
    ]

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
        provids: List[str],
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
    ) -> MPCObservations:
        """
        Query the MPC database for the observations and associated data for the given
        provisional designations.

        Parameters
        ----------
        provids : List[str]
            List of provisional designations to query.
        include_columns : Optional[List[str]]
            Columns to include from `public_obs_sbn`. If None, includes a sensible default set.
            Use together with exclude_columns; include is applied first.
        exclude_columns : Optional[List[str]]
            Columns to exclude from the final selection. Applied after include_columns.

        Returns
        -------
        observations : MPCObservations
            The observations and associated data for the given provisional designations.
        """
        provids_str = ", ".join([f'"{id}"' for id in provids])

        # Map MPCObservations fields to SQL select expressions (with casts/aliases)
        field_sql: Dict[str, str] = {
            # Identity and linkage
            "id": "obs_sbn.id AS id",
            "obsid": "obs_sbn.obsid AS obsid",
            "trksub": "obs_sbn.trksub AS trksub",
            "trkid": "obs_sbn.trkid AS trkid",
            "provid": "obs_sbn.provid AS provid",
            "permid": "obs_sbn.permid AS permid",
            "submission_id": "obs_sbn.submission_id AS submission_id",
            "submission_block_id": "obs_sbn.submission_block_id AS submission_block_id",
            "obssubid": "obs_sbn.obssubid AS obssubid",
            # Raw content
            "obs80": "obs_sbn.obs80 AS obs80",
            "status": "obs_sbn.status AS status",
            "ref": "obs_sbn.ref AS ref",
            "healpix": "obs_sbn.healpix AS healpix",
            "artsat": "obs_sbn.artsat AS artsat",
            "mode": "obs_sbn.mode AS mode",
            "stn": "obs_sbn.stn AS stn",
            "trx": "obs_sbn.trx AS trx",
            "rcv": "obs_sbn.rcv AS rcv",
            "sys": "obs_sbn.sys AS sys",
            "ctr": "obs_sbn.ctr AS ctr",
            "pos1": "obs_sbn.pos1 AS pos1",
            "pos2": "obs_sbn.pos2 AS pos2",
            "pos3": "obs_sbn.pos3 AS pos3",
            "poscov11": "obs_sbn.poscov11 AS poscov11",
            "poscov12": "obs_sbn.poscov12 AS poscov12",
            "poscov13": "obs_sbn.poscov13 AS poscov13",
            "poscov22": "obs_sbn.poscov22 AS poscov22",
            "poscov23": "obs_sbn.poscov23 AS poscov23",
            "poscov33": "obs_sbn.poscov33 AS poscov33",
            "prog": "obs_sbn.prog AS prog",
            # Time and geometry
            "obstime": "obs_sbn.obstime AS obstime",
            "obstime_text": "obs_sbn.obstime_text AS obstime_text",
            "ra": "CAST(obs_sbn.ra AS FLOAT64) AS ra",
            "dec": "CAST(obs_sbn.dec AS FLOAT64) AS dec",
            "rastar": "obs_sbn.rastar AS rastar",
            "decstar": "obs_sbn.decstar AS decstar",
            "obscenter": "obs_sbn.obscenter AS obscenter",
            "deltara": "obs_sbn.deltara AS deltara",
            "deltadec": "obs_sbn.deltadec AS deltadec",
            "dist": "obs_sbn.dist AS dist",
            "pa": "obs_sbn.pa AS pa",
            "rmsra": "CAST(obs_sbn.rmsra AS FLOAT64) AS rmsra",
            "rmsdec": "CAST(obs_sbn.rmsdec AS FLOAT64) AS rmsdec",
            "rmscorr": "CAST(obs_sbn.rmscorr AS FLOAT64) AS rmscorr",
            "rmsdist": "obs_sbn.rmsdist AS rmsdist",
            "rmspa": "obs_sbn.rmspa AS rmspa",
            "delay": "obs_sbn.delay AS delay",
            "rmsdelay": "obs_sbn.rmsdelay AS rmsdelay",
            "doppler": "obs_sbn.doppler AS doppler",
            "rmsdoppler": "obs_sbn.rmsdoppler AS rmsdoppler",
            # Photometry
            "astcat": "obs_sbn.astcat AS astcat",
            "mag": "CAST(obs_sbn.mag AS FLOAT64) AS mag",
            "rmsmag": "CAST(obs_sbn.rmsmag AS FLOAT64) AS rmsmag",
            "band": "obs_sbn.band AS band",
            "fltr": "obs_sbn.fltr AS fltr",
            "photcat": "obs_sbn.photcat AS photcat",
            "photap": "obs_sbn.photap AS photap",
            "nucmag": "obs_sbn.nucmag AS nucmag",
            "logsnr": "obs_sbn.logsnr AS logsnr",
            "seeing": "obs_sbn.seeing AS seeing",
            "exp": "obs_sbn.exp AS exp",
            "rmsfit": "obs_sbn.rmsfit AS rmsfit",
            # Misc
            "com": "obs_sbn.com AS com",
            "frq": "obs_sbn.frq AS frq",
            "disc": "obs_sbn.disc AS disc",
            "subfrm": "obs_sbn.subfrm AS subfrm",
            "subfmt": "obs_sbn.subfmt AS subfmt",
            "prectime": "obs_sbn.prectime AS prectime",
            "precra": "obs_sbn.precra AS precra",
            "precdec": "obs_sbn.precdec AS precdec",
            "unctime": "obs_sbn.unctime AS unctime",
            "notes": "obs_sbn.notes AS notes",
            "remarks": "obs_sbn.remarks AS remarks",
            "deprecated": "obs_sbn.deprecated AS deprecated",
            "localuse": "obs_sbn.localuse AS localuse",
            "nstars": "obs_sbn.nstars AS nstars",
            "prev_desig": "obs_sbn.prev_desig AS prev_desig",
            "prev_ref": "obs_sbn.prev_ref AS prev_ref",
            "rmstime": "obs_sbn.rmstime AS rmstime",
            "created_at": "obs_sbn.created_at AS created_at",
            "updated_at": "obs_sbn.updated_at AS updated_at",
            "trkmpc": "obs_sbn.trkmpc AS trkmpc",
            "orbit_id": "obs_sbn.orbit_id AS orbit_id",
            "designation_asterisk": "obs_sbn.designation_asterisk AS designation_asterisk",
            "all_pub_ref": "CAST(obs_sbn.all_pub_ref AS STRING) AS all_pub_ref",
            "shapeocc": "obs_sbn.shapeocc AS shapeocc",
            "replacesobsid": "obs_sbn.replacesobsid AS replacesobsid",
            "group_id": "obs_sbn.group_id AS group_id",
            "datastream_metadata_uuid": "obs_sbn.datastream_metadata.uuid AS datastream_metadata_uuid",
            "datastream_metadata_source_timestamp": "obs_sbn.datastream_metadata.source_timestamp AS datastream_metadata_source_timestamp",
            "vel1": "obs_sbn.vel1 AS vel1",
            "vel2": "obs_sbn.vel2 AS vel2",
            "vel3": "obs_sbn.vel3 AS vel3",
        }



        selected_columns: List[str] = list(include_columns) if include_columns else self.default_columns
        if exclude_columns:
            exclude_set = set(exclude_columns)
            selected_columns = [c for c in selected_columns if c not in exclude_set]

        # Always add requested_provid and primary_designation unless explicitly excluded
        base_meta = [
            ("requested_provid", "rp.provid AS requested_provid"),
            (
                "primary_designation",
                "CASE WHEN ni.permid IS NOT NULL THEN ni.permid ELSE ci.unpacked_primary_provisional_designation END AS primary_designation",
            ),
        ]
        select_clauses: List[str] = [expr for name, expr in base_meta if (not exclude_columns or name not in exclude_columns)]

        for col in selected_columns:
            if col in field_sql:
                select_clauses.append(field_sql[col])

        select_sql = ",\n            ".join(select_clauses)

        query = f"""
        WITH requested_provids AS (
            SELECT provid
            FROM UNNEST(ARRAY[{provids_str}]) AS provid
        )
        SELECT DISTINCT
            {select_sql}
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
        ORDER BY rp.provid ASC, obs_sbn.obstime ASC;
        """

        query_job = self.client.query(query)
        results = query_job.result()
        table = results.to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True).combine_chunks()

        kwargs: Dict[str, Any] = {}

        # Helper to check presence
        def has(col: str) -> bool:
            return col in table.column_names

        # Meta
        if has("requested_provid"):
            kwargs["requested_provid"] = table["requested_provid"]
        if has("primary_designation"):
            kwargs["primary_designation"] = table["primary_designation"]

        # Timestamp conversions
        if has("obstime"):
            obstime = Time(table["obstime"].to_numpy(zero_copy_only=False), format="datetime64", scale="utc")
            kwargs["obstime"] = Timestamp.from_astropy(obstime)
        if has("created_at"):
            created_at = Time(table["created_at"].to_numpy(zero_copy_only=False), format="datetime64", scale="utc")
            kwargs["created_at"] = Timestamp.from_astropy(created_at)
        if has("updated_at"):
            updated_at = Time(table["updated_at"].to_numpy(zero_copy_only=False), format="datetime64", scale="utc")
            kwargs["updated_at"] = Timestamp.from_astropy(updated_at)

        # Direct pass-through for remaining selected columns present
        passthrough_columns = [
            c
            for c in table.column_names
            if c
            not in {
                "requested_provid",
                "primary_designation",
                "obstime",
                "created_at",
                "updated_at",
            }
        ]
        for c in passthrough_columns:
            kwargs[c] = table[c]

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

        # Handle NULL values in the epoch_mjd column: ideally
        # we should have the Timestamp class be able to handle this
        mjd_array = table["epoch_mjd"].to_numpy(zero_copy_only=False)
        mjds = np.ma.masked_array(mjd_array, mask=np.isnan(mjd_array))  # type: ignore
        epoch = Time(mjds, format="mjd", scale="tt")

        return MPCOrbits.from_kwargs(
            # Note, since we didn't request a specific provid we use the one MPC provides
            requested_provid=table["provid"],
            id=table["id"],
            provid=table["provid"],
            epoch=Timestamp.from_astropy(epoch),
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
            created_at=Timestamp.from_astropy(created_at),
            updated_at=Timestamp.from_astropy(updated_at),
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
        LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci
            ON ci.unpacked_secondary_provisional_designation = rp.provid
        LEFT JOIN `{self.dataset_id}.public_current_identifications` AS ci_alt
            ON ci.unpacked_primary_provisional_designation = ci_alt.unpacked_primary_provisional_designation
        LEFT JOIN `{self.dataset_id}.public_numbered_identifications` AS ni
            ON ci.unpacked_primary_provisional_designation = ni.unpacked_primary_provisional_designation
        LEFT JOIN `{self.dataset_id}.public_mpc_orbits` AS mpc_orbits
            ON ci.unpacked_primary_provisional_designation = mpc_orbits.unpacked_primary_provisional_designation
        ORDER BY 
            requested_provid ASC,
            mpc_orbits.epoch_mjd ASC;
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

        # Handle NULL values in the epoch_mjd column: ideally
        # we should have the Timestamp class be able to handle this
        mjd_array = table["epoch_mjd"].to_numpy(zero_copy_only=False)
        mjds = np.ma.masked_array(mjd_array, mask=np.isnan(mjd_array))  # type: ignore
        epoch = Time(mjds, format="mjd", scale="tt")

        return MPCOrbits.from_kwargs(
            requested_provid=table["requested_provid"],
            primary_designation=table["primary_designation"],
            id=table["id"],
            provid=table["provid"],
            epoch=Timestamp.from_astropy(epoch),
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
            created_at=Timestamp.from_astropy(created_at),
            updated_at=Timestamp.from_astropy(updated_at),
        )

    def query_submission_info(self, submission_ids: List[str]) -> MPCSubmissionResults:
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
        )

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
            created_at=Timestamp.from_astropy(created_at),
            updated_at=Timestamp.from_astropy(updated_at),
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
                    matched_results["separation_seconds"].to_numpy(
                        zero_copy_only=False
                    ),
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
        results = (
            self.client.query(final_query)
            .result()
            .to_arrow(progress_bar_type="tqdm", create_bqstorage_client=True)
        )

        # Defragment the pyarrow table first
        results = results.combine_chunks()
        obstime = Time(
            results["obstime"].to_numpy(zero_copy_only=False),
            format="datetime64",
            scale="utc",
        )
        created_at = Time(
            results["created_at"].to_numpy(zero_copy_only=False),
            format="datetime64",
            scale="utc",
        )
        updated_at = Time(
            results["updated_at"].to_numpy(zero_copy_only=False),
            format="datetime64",
            scale="utc",
        )

        separation_arcseconds = (
            results["separation_meters"].to_numpy(zero_copy_only=False)
            * METERS_PER_ARCSECONDS
        )

        return CrossMatchedMPCObservations.from_kwargs(
            request_id=results["input_id"],
            separation_arcseconds=separation_arcseconds,
            separation_seconds=results["separation_seconds"],
            mpc_observations=MPCObservations.from_kwargs(
                id=results["id"],
                obsid=results["obsid"],
                trksub=results["trksub"],
                provid=results["provid"],
                permid=results["permid"],
                submission_id=results["submission_id"],
                obssubid=results["obssubid"],
                obstime=Timestamp.from_astropy(obstime),
                ra=results["ra"],
                dec=results["dec"],
                rmsra=results["rmsra"],
                rmsdec=results["rmsdec"],
                mag=results["mag"],
                rmsmag=results["rmsmag"],
                band=results["band"],
                stn=results["stn"],
                updated_at=Timestamp.from_astropy(updated_at),
                created_at=Timestamp.from_astropy(created_at),
                status=results["status"],
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
            b.id,
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

        # Convert timestamps
        obstime = Time(
            results["obstime"].to_numpy(zero_copy_only=False),
            format="datetime64",
            scale="utc",
        )
        created_at = Time(
            results["created_at"].to_numpy(zero_copy_only=False),
            format="datetime64",
            scale="utc",
        )
        updated_at = Time(
            results["updated_at"].to_numpy(zero_copy_only=False),
            format="datetime64",
            scale="utc",
        )

        # Convert meters to arcseconds
        separation_arcseconds = (
            results["separation_meters"].to_numpy(zero_copy_only=False)
            / METERS_PER_ARCSECONDS
        )

        return CrossMatchedMPCObservations.from_kwargs(
            request_id=results["input_id"].cast(pa.string()),
            separation_arcseconds=separation_arcseconds,
            separation_seconds=results["separation_seconds"],
            mpc_observations=MPCObservations.from_kwargs(
                id=results["id"],
                obsid=results["obsid"],
                trksub=results["trksub"],
                provid=results["provid"],
                permid=results["permid"],
                submission_id=results["submission_id"],
                obssubid=results["obssubid"],
                obstime=Timestamp.from_astropy(obstime),
                ra=results["ra"],
                dec=results["dec"],
                rmsra=results["rmsra"],
                rmsdec=results["rmsdec"],
                mag=results["mag"],
                rmsmag=results["rmsmag"],
                band=results["band"],
                stn=results["stn"],
                updated_at=Timestamp.from_astropy(updated_at),
                created_at=Timestamp.from_astropy(created_at),
                status=results["status"],
            ),
        )
