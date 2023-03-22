import json
import logging
from typing import Iterator, Optional

import astropy.time
import google.cloud.secretmanager
import sqlalchemy as sq
from google.cloud.sql.connector import Connector

from .observation import Observation, ObservationStatus

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
        cloudsql_connection_name: str = "moeyens-thor-dev:us-west1:mpc-replica",
        credentials_uri: str = "projects/moeyens-thor-dev/secrets/mpc-replica-readonly-credentials/versions/latest",  # noqa: E501
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

    @staticmethod
    def _parse_obs_sbn_row(row: sq.engine.Row) -> Observation:
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
