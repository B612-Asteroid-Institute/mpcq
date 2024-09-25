from abc import ABC, abstractmethod
from typing import Any, List

from adam_core.time import Timestamp
from astropy.time import Time
from google.cloud import bigquery

from .observations import MPCObservations
from .orbits import MPCOrbits
from .submissions import MPCSubmissionInfo


class MPCClient(ABC):

    @abstractmethod
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
    def query_submission_info(self, submission_ids: List[str]) -> MPCSubmissionInfo:
        """
        Query for observation status and mapping (observation ID to trksub, provid, etc.) for a
        given list of submission IDs.

        Parameters
        ----------
        submission_ids : List[str]
            List of submission IDs to query.

        Returns
        -------
        submission_info : MPCSubmissionInfo
            The observation status and mapping for the given submission IDs.
        """
        pass


class BigQueryMPCClient(MPCClient):

    def __init__(self, **kwargs: dict[str, Any]) -> None:
        self.client = bigquery.Client(**kwargs)

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
        provids_str = ", ".join([f'"{id}"' for id in provids])

        query = f"""WITH provid_mapping AS (
            SELECT
                unpacked_primary_provisional_designation AS primary_designation,
                unpacked_primary_provisional_designation AS provid
            FROM `moeyens-thor-dev.mpc_sbn_aipublic.current_identifications`
            WHERE unpacked_primary_provisional_designation IN ({provids_str})
            
            UNION DISTINCT
            
            SELECT
                unpacked_primary_provisional_designation AS primary_designation,
                unpacked_secondary_provisional_designation AS provid
            FROM `moeyens-thor-dev.mpc_sbn_aipublic.current_identifications`
            WHERE unpacked_secondary_provisional_designation IN ({provids_str})
        ),
        permid_mapping AS (
            SELECT
                num_ident.permid,
                pm.primary_designation
            FROM provid_mapping AS pm
            LEFT JOIN `moeyens-thor-dev.mpc_sbn_aipublic.numbered_identifications` AS num_ident
                ON pm.primary_designation = num_ident.unpacked_primary_provisional_designation
        )
        SELECT DISTINCT
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
            obs_sbn.mag, 
            obs_sbn.rmsmag, 
            obs_sbn.band, 
            obs_sbn.stn, 
            obs_sbn.updated_at, 
            obs_sbn.created_at, 
            obs_sbn.status,
            CASE
                WHEN obs_sbn.permid IS NOT NULL THEN obs_sbn.permid
                ELSE pm.primary_designation
            END AS primary_designation
        FROM `moeyens-thor-dev.mpc_sbn_aipublic.obs_sbn` AS obs_sbn
        LEFT JOIN provid_mapping AS pm
            ON obs_sbn.provid = pm.provid
        LEFT JOIN permid_mapping AS pdm
            ON obs_sbn.permid = pdm.permid
        WHERE obs_sbn.provid IN (SELECT provid FROM provid_mapping)
        OR obs_sbn.permid IN (SELECT permid FROM permid_mapping)
        ORDER BY primary_designation ASC, obs_sbn.obstime ASC;
        """
        query_job = self.client.query(query)
        results = query_job.result()
        table = results.to_arrow()

        obstime = Time(
            table["obstime"].to_numpy(zero_copy_only=False),
            format="datetime64",
            scale="utc",
        )
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

        return MPCObservations.from_kwargs(
            obsid=table["obsid"],
            primary_designation=table["primary_designation"],
            trksub=table["trksub"],
            provid=table["provid"],
            permid=table["permid"],
            submission_id=table["submission_id"],
            obssubid=table["obssubid"],
            obstime=Timestamp.from_astropy(obstime),
            ra=table["ra"],
            dec=table["dec"],
            rmsra=table["rmsra"],
            rmsdec=table["rmsdec"],
            mag=table["mag"],
            rmsmag=table["rmsmag"],
            band=table["band"],
            stn=table["stn"],
            updated_at=Timestamp.from_astropy(updated_at),
            created_at=Timestamp.from_astropy(created_at),
            status=table["status"],
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
        WITH provid_mapping AS (
            SELECT
                unpacked_primary_provisional_designation AS primary_designation,
                unpacked_primary_provisional_designation AS provid
            FROM `moeyens-thor-dev.mpc_sbn_aipublic.current_identifications`
            WHERE unpacked_primary_provisional_designation IN ({provids_str})
            
            UNION DISTINCT
            
            SELECT
                unpacked_primary_provisional_designation AS primary_designation,
                unpacked_secondary_provisional_designation AS provid
            FROM `moeyens-thor-dev.mpc_sbn_aipublic.current_identifications`
            WHERE unpacked_secondary_provisional_designation IN ({provids_str})
        ),
        permid_mapping AS (
            SELECT
                num_ident.permid,
                num_ident.unpacked_primary_provisional_designation AS provid,
                pm.primary_designation
            FROM provid_mapping AS pm
            LEFT JOIN `moeyens-thor-dev.mpc_sbn_aipublic.numbered_identifications` AS num_ident
                ON pm.primary_designation = num_ident.unpacked_primary_provisional_designation
        )
        SELECT DISTINCT 
            mpc_orbits.id, 
            pdm.permid AS permid,
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
            mpc_orbits.updated_at,
            CASE
                WHEN pdm.permid IS NOT NULL THEN pdm.permid
                ELSE pm.primary_designation
            END AS primary_designation
        FROM `moeyens-thor-dev.mpc_sbn_aipublic.mpc_orbits` AS mpc_orbits
        LEFT JOIN provid_mapping AS pm
            ON mpc_orbits.unpacked_primary_provisional_designation = pm.provid
        LEFT JOIN permid_mapping AS pdm
            ON mpc_orbits.unpacked_primary_provisional_designation = pdm.provid
        WHERE mpc_orbits.unpacked_primary_provisional_designation IN (SELECT provid FROM provid_mapping)
        OR mpc_orbits.unpacked_primary_provisional_designation IN (SELECT provid FROM permid_mapping)
        ORDER BY 
            primary_designation ASC,
            mpc_orbits.epoch_mjd ASC
        """
        query_job = self.client.query(query)
        results = query_job.result()
        table = results.to_arrow()

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

        return MPCOrbits.from_kwargs(
            id=table["id"],
            primary_designation=table["primary_designation"],
            provid=table["provid"],
            permid=table["permid"],
            epoch=Timestamp.from_mjd(table["epoch_mjd"], scale="tt"),
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

    def query_submission_info(self, submission_ids: List[str]) -> MPCSubmissionInfo:
        """
        Query for observation status and mapping (observation ID to trksub, provid, etc.) for a
        given list of submission IDs.

        Parameters
        ----------
        submission_ids : List[str]
            List of submission IDs to query.

        Returns
        -------
        submission_info : MPCSubmissionInfo
            The observation status and mapping for the given submission IDs.
        """
        submission_ids_str = ", ".join([f'"{id}"' for id in submission_ids])
        query = f"""
        SELECT DISTINCT
            obs_sbn.obsid,
            obs_sbn.obssubid,
            primary_designation,
            obs_sbn.trksub,
            obs_sbn.provid,
            obs_sbn.permid,
            obs_sbn.submission_id,
            obs_sbn.status
        FROM
            `moeyens-thor-dev.mpc_sbn_aipublic.obs_sbn` AS obs_sbn
        LEFT JOIN (
            SELECT
                unpacked_primary_provisional_designation AS primary_designation,
                unpacked_secondary_provisional_designation AS secondary_designation
            FROM
                `moeyens-thor-dev.mpc_sbn_aipublic.current_identifications`
            WHERE
                unpacked_primary_provisional_designation IN (
                    SELECT DISTINCT provid
                    FROM `moeyens-thor-dev.mpc_sbn_aipublic.obs_sbn`
                    WHERE submission_id IN ({submission_ids_str})
                )
                OR unpacked_secondary_provisional_designation IN (
                    SELECT DISTINCT provid
                    FROM `moeyens-thor-dev.mpc_sbn_aipublic.obs_sbn`
                    WHERE submission_id IN ({submission_ids_str})
                )
        ) AS identifications
        ON
            obs_sbn.provid = identifications.primary_designation
            OR obs_sbn.provid = identifications.secondary_designation
        WHERE
            obs_sbn.submission_id IN ({submission_ids_str})
        ORDER BY
            obs_sbn.submission_id ASC,
            obs_sbn.obsid ASC
        """
        query_job = self.client.query(query)
        results = query_job.result()
        table = results.to_arrow()

        return MPCSubmissionInfo.from_pyarrow(table)
