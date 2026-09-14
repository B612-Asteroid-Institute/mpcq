from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

from adam_core.observations import ADESObservations

from ..observations import CrossMatchedMPCObservations, MPCObservations
from ..orbits import MPCOrbits, MPCPrimaryObjects
from ..submissions.types import (
    MPCSubmissionHistory,
    MPCSubmissionResults,
)

METERS_PER_ARCSECONDS = 30.87
ObservationColumnMode = Literal["minimal", "ades", "full"]
OrbitColumnMode = Literal["minimal", "full"]


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

    @abstractmethod
    def query_submission_results(self, submission_ids: list[str]) -> MPCSubmissionResults:
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

    @abstractmethod
    def query_submission_num_obs(self, submission_id: str) -> int:
        """
        Queries the number of observations in a given submission.

        Parameters
        ----------
        submission_id : str
            The submission ID to query.

        Returns
        -------
        int
            The number of observations in the submission.
        """
