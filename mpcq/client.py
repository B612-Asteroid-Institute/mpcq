import json
import logging
from datetime import datetime
from typing import Iterator, Optional, Any, Dict, List

import astropy.time
import google.cloud.secretmanager
import numpy as np
import sqlalchemy as sq
from google.cloud.sql.connector import Connector

from .observation import Observation, ObservationsQv, ObservationStatus
from .submission import Submission

log = logging.getLogger("mpcq.client")

_DB_DRIVER = "pg8000"


class MPCObservationsClient:
    def __init__(self, dbconn: sq.engine.Connection):
        self._dbconn = dbconn

    @classmethod
    def connect(cls, engine: sq.engine.Engine):
        return cls(dbconn=engine.connect())

    @classmethod
    def connect_using_gcloud(
        cls,
        cloudsql_connection_name: str = "moeyens-thor-dev:us-west1:mpc-sbn-replica",
        credentials_uri: str = "projects/moeyens-thor-dev/secrets/mpc-sbn-replica-readonly-credentials/versions/latest",  # noqa: E501
    ):
        log.info("loading database credentials")
        client = google.cloud.secretmanager.SecretManagerServiceClient()
        secret = client.access_secret_version(name=credentials_uri)
        creds = json.loads(secret.payload.data)
        log.info("database credentials loaded successfully")

        connector = Connector()

        def make_connection():
            conn = connector.connect(
                cloudsql_connection_name,
                _DB_DRIVER,
                user=creds["username"],
                password=creds["password"],
                db="mpc_sbn",
            )
            return conn

        engine = sq.create_engine(
            f"postgresql+{_DB_DRIVER}://", creator=make_connection
        )
        return cls.connect(engine)

    def close(self):
        self._dbconn.close()

    def get_object_observations(
        self,
        object_provisional_designation: str,
        obscode: Optional[str] = None,
        filter_band: Optional[str] = None,
    ) -> Iterator[Observation]:
        stmt = self._observations_select_stmt(
            object_provisional_designation, obscode, filter_band
        )
        result = self._dbconn.execute(stmt)
        for r in result:
            yield self._parse_obs_sbn_row(r)

    def get_object_submissions(
        self,
        object_provisional_designation: str,
    ) -> Iterator[Submission]:
        stmt = self._submissions_select_stmt(object_provisional_designation)
        result = self._dbconn.execute(stmt)
        for r in result:
            yield self._parse_submissions_sbn_row(r)

    @staticmethod
    def _parse_obs_sbn_row(row: sq.engine.Row) -> Observation:
        if row.created_at is None:
            created_at = None
        else:
            created_at = astropy.time.Time(row.created_at, scale="utc")

        if row.updated_at is None:
            updated_at = None
        else:
            updated_at = astropy.time.Time(row.updated_at, scale="utc")

        return Observation(
            mpc_id=row.id,
            status=ObservationStatus._from_db_value(row.status),
            obscode=row.stn,
            filter_band=row.band,
            unpacked_provisional_designation=row.provid,
            timestamp=astropy.time.Time(row.obstime, scale="utc"),
            ra=row.ra,
            dec=row.dec,
            mag=row.mag,
            ra_rms=row.rmsra,
            dec_rms=row.rmsdec,
            mag_rms=row.rmsmag,
            submission_id=row.submission_id,
            created_at=created_at,
            updated_at=updated_at,
        )

    @staticmethod
    def _parse_submissions_sbn_row(row: sq.engine.Row) -> Submission:
        return Submission(
            id=row.submission_id,
            num_observations=row.num_observations,
        )

    def _observations_select_stmt(
        self,
        provisional_designation: str,
        obscode: Optional[str],
        filter_band: Optional[str],
    ) -> sq.sql.expression.Select:
        """Construct a database select statement to fetch observations for
        given object (named by provisional designation, eg "2022 AJ2").

        obscode and filter_band can optionally be provided to limit the
        result set.

        """
        log.info("loading observations for %s", provisional_designation)
        stmt = (
            sq.select(
                sq.column("id"),
                sq.column("stn"),
                sq.column("status"),
                sq.column("ra"),
                sq.column("dec"),
                sq.column("obstime"),
                sq.column("provid"),
                sq.column("rmsra"),
                sq.column("rmsdec"),
                sq.column("mag"),
                sq.column("rmsmag"),
                sq.column("band"),
                sq.column("submission_id"),
                sq.column("created_at"),
                sq.column("updated_at"),
            )
            .select_from(sq.table("obs_sbn"))
            .where(
                sq.column("provid") == provisional_designation,
            )
        )
        if obscode is not None:
            stmt = stmt.where(sq.column("stn") == obscode)
        if filter_band is not None:
            stmt = stmt.where(sq.column("band") == filter_band)

        return stmt

    def _submissions_select_stmt(
        self,
        provisional_designation: str,
    ) -> sq.sql.expression.Select:
        """
        Construct a database select statement to fetch submission IDs for
        the given object (named by provisional designation, eg "2022 AJ2").

        Parameters
        ----------
        provisional_designation : str
            The provisional designation of the object to fetch submissions for.

        Returns
        -------
        sq.sql.expression.Select
            The select statement to fetch the submission IDs for the given object.
        """
        log.info("loading submissions for %s", provisional_designation)
        stmt = (
            sq.select(
                sq.column("submission_id"),
                sq.func.count(sq.column("submission_id")).label("num_observations"),
            )
            .select_from(sq.table("obs_sbn"))
            .where(
                sq.column("provid") == provisional_designation,
            )
            .group_by(sq.column("submission_id"))
        )

        return stmt

    def object_counts_for_submission_id(
        self, submission_id: str
    ) -> list[tuple[str, int]]:
        """
        Queries for the number of observations for each object associated with a given submission ID.
        """
        log.info("loading object counts for submission ID %s", submission_id)
        stmt = (
            sq.select(
                sq.column("provid"),
                sq.func.count(sq.column("provid")).label("num_observations"),
            )
            .select_from(sq.table("obs_sbn"))
            .where(
                # HACK: the database currently only has an index for
                # submission_block_id, even though we'd like to use
                # submission_id.
                sq.column("submission_block_id") == submission_id + "_01",
                # We'd prefer this:
                # sq.column("submission_id") == submission_id,
                # but that index is still being built.
            )
            .group_by(sq.column("provid"))
        )
        result = self._dbconn.execute(stmt)
        return [(row.provid, row.num_observations) for row in result]

    def observations_table(
        self, obscodes: list[str], start_timestamp: datetime, end_timestamp: datetime
    ) -> ObservationsQv:
        """
        Queries for all observations within a given time range for a given set of observatory codes.
        """
        # log.info("loading observations for obscodes ID %s", obscodes)
        stmt = (
            sq.select(
                sq.column("id").label("mpc_id"),
                sq.column("stn"),
                sq.column("status"),
                sq.column("ra"),
                sq.column("dec"),
                sq.column("obstime"),
                sq.column("permid"),
                sq.column("provid"),
                sq.column("rmsra"),
                sq.column("rmsdec"),
                sq.column("mag"),
                sq.column("rmsmag"),
                sq.column("band"),
                sq.column("submission_id"),
                sq.column("created_at"),
                sq.column("updated_at"),
            )
            .select_from(sq.table("obs_sbn"))
            .where(
                sq.and_(
                    sq.column("obstime").between(start_timestamp, end_timestamp),
                    sq.column("stn").in_(obscodes),
                )
            )
        )
        result = self._dbconn.execute(stmt)

        # Create lists to store the data
        data: Dict[str, List[Any]] = {
            "mpc_id": [],
            "stn": [],
            "status": [],
            "ra": [],
            "dec": [],
            "obstime": [],
            "permid": [],
            "provid": [],
            "rmsra": [],
            "rmsdec": [],
            "mag": [],
            "rmsmag": [],
            "band": [],
            "submission_id": [],
            "created_at": [],
            "updated_at": [],
        }

        # Iterate over the result and store the data in lists
        for row in result:
            for column, value in zip(data.keys(), row):
                # we'll cast times to mjd for convenience
                if column in ["created_at", "updated_at"]:
                    if value:
                        data[column].append(astropy.time.Time(value, scale="utc").mjd)
                    else:
                        data[column].append(None)
                else:
                    data[column].append(value)
        data["obstime_mjd"] = [
            astropy.time.Time(x, scale="utc").mjd for x in data["obstime"]
        ]

        return ObservationsQv.from_kwargs(
            mpc_id=np.array(data["mpc_id"], dtype=np.int64),
            status=np.array(data["status"]),
            obscode=np.array(data["stn"]),
            filter_band=np.array(data["band"]),
            permanent_designation=np.array(data["permid"]),
            unpacked_provisional_designation=np.array(data["provid"]),
            mjd=np.array(data["obstime_mjd"], dtype=np.float64),
            timestamp=np.array(data["obstime"], dtype=np.datetime64),
            ra=np.array(data["ra"], dtype=np.float64),
            ra_sigma=np.array(data["rmsra"], dtype=np.float64),
            dec=np.array(data["dec"], dtype=np.float64),
            dec_sigma=np.array(data["rmsdec"], dtype=np.float64),
            mag=np.array(data["mag"], dtype=np.float64),
            mag_rms=np.array(data["rmsmag"], dtype=np.float64),
            created_at=np.array(data["created_at"]),
            updated_at=np.array(data["updated_at"]),
            submission_id=np.array(data["submission_id"]),
        )
