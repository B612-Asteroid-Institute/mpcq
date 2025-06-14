import uuid
from datetime import datetime
from typing import Iterator, List, Literal, Optional

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import quivr as qv
from adam_core.observations import ADESObservations, SourceCatalog
from adam_core.time import Timestamp

from .types import (
    AssociationCandidates,
    AssociationMembers,
    DiscoveryCandidateMembers,
    DiscoveryCandidates,
)


def round_to_nearest_millisecond(t: datetime) -> datetime:
    microseconds = np.ceil(t.microsecond / 1000).astype(int) * 1000
    return t.replace(microsecond=microseconds)


def split_into_max_size(
    table: qv.AnyTable, column: str, max_size: int
) -> Iterator[qv.AnyTable]:
    """
    Split a table into smaller tables of a maximum size number of rows. Unique values within the user-defined
    column will be kept together in the same chunk.

    Parameters
    ----------
    table : qv.AnyTable
        The table to split into chunks.
    column : str
        The column to group by.
    max_size : int
        The maximum size of each table.

    Returns
    -------
    Iterator[qv.AnyTable]
        An iterator over the tables.
    """
    grouped_table = (
        table.flattened_table().group_by(column).aggregate([(column, "count")])
    )

    cumulative_count = pc.cumulative_sum(grouped_table[f"{column}_count"])
    chunk = pc.divide(cumulative_count, max_size)
    grouped_table = grouped_table.append_column("chunk", chunk)

    for chunk in grouped_table["chunk"].unique():

        values_in = grouped_table.filter(
            pc.equal(grouped_table["chunk"], chunk)
        ).column(column)

        yield table.apply_mask(pc.is_in(table.table.column(column), values_in))


def generate_submission_id(
    submission_type: Literal["discovery", "association", "identification"],
    prefix: Optional[str] = None,
) -> str:
    """
    Generate a submission ID based on the submission type and the current date.

    Parameters
    ----------
    submission_type : Literal["discovery", "association", "identification"]
        The type of submission to generate an ID for.

    Returns
    -------
    str
        The generated submission ID.
    """
    if prefix is None:
        return f"{submission_type[0]}{uuid.uuid4().hex[:5]}"
    else:
        return f"{prefix}_{submission_type[0]}{uuid.uuid4().hex[:5]}"


def candidates_to_ades(
    candidates: qv.AnyTable,
    members: qv.AnyTable,
    source_catalog: SourceCatalog,
    max_observations_per_table: Optional[int] = 1000000,
) -> List[ADESObservations]:
    """
    Convert candidates and their members to ADESObservations.

    Parameters
    ----------
    candidates : qv.AnyTable
        The candidates table.
    members : qv.AnyTable
        The members table.
    source_catalog : SourceCatalog
        The source catalog.
    max_observations_per_table : Optional[int], optional
        The maximum number of observations per table, by default 1000000

    Returns
    -------
    List[ADESObservations]
        The ADESObservations.
    """
    assert pc.all(
        pc.is_in(members.obssubid, source_catalog.id)
    ).as_py(), "All obssubids in members must be present in source_catalog."
    assert pc.all(
        pc.is_in(candidates.trksub, members.trksub)
    ).as_py(), "All trksubs in candidates must be present in members."
    assert pc.all(
        pc.is_in(members.trksub, candidates.trksub)
    ).as_py(), "All trksubs in members must be present in candidates."

    members_table = members.flattened_table().select(["trksub", "obssubid"])
    members_observations = members_table.join(
        source_catalog.flattened_table(), "obssubid", "id"
    )
    members_observations = members_observations.combine_chunks()

    ades = ADESObservations.from_kwargs(
        # permID=,
        # provID=,
        trkSub=members_observations.column("trksub"),
        obsSubID=members_observations.column("obssubid"),
        obsTime=Timestamp.from_kwargs(
            days=members_observations.column("time.days"),
            nanos=members_observations.column("time.nanos"),
            scale=source_catalog.time.scale,
        ),
        # rmsTime=,
        ra=members_observations.column("ra"),
        dec=members_observations.column("dec"),
        rmsRACosDec=pc.multiply(
            members_observations.column("ra_sigma"),
            pa.array(np.cos(np.radians(members_observations.column("dec")))),
        ),
        rmsDec=members_observations.column("dec_sigma"),
        rmsCorr=members_observations.column("radec_corr"),
        mag=members_observations.column("mag"),
        rmsMag=members_observations.column("mag_sigma"),
        band=members_observations.column("filter"),
        stn=members_observations.column("observatory_code"),
        mode=pa.repeat("CCD", len(members_observations)),
        astCat=members_observations.column("astrometric_catalog"),
        photCat=members_observations.column("photometric_catalog"),
        logSNR=pc.log10(members_observations.column("snr")),
        seeing=members_observations.column("exposure_seeing"),
        exp=members_observations.column("exposure_duration"),
        # remarks=
    )

    ades_tables = []
    for chunk in split_into_max_size(ades, "trkSub", max_observations_per_table):
        ades_tables.append(chunk)

    return ades_tables


def prepare_submission_tables(
    source_catalog: SourceCatalog,
    discovery_candidates: Optional[DiscoveryCandidates] = None,
    discovery_candidate_members: Optional[DiscoveryCandidateMembers] = None,
    association_candidates: Optional[AssociationCandidates] = None,
    association_members: Optional[AssociationMembers] = None,
    max_observations_per_file: Optional[int] = 10000,
) -> tuple[List[ADESObservations], List[ADESObservations]]:
    """
    Generate ADESObservation tables for the given candidates and their members.

    Parameters
    ----------
    source_catalog : SourceCatalog
        The source catalog.
    discovery_candidates : Optional[DiscoveryCandidates], optional
        The discovery candidates. These are objects believed to be newly observed.
    discovery_candidate_members : Optional[DiscoveryCandidateMembers], optional
        The observation members of the discovery candidates.
    association_candidates : Optional[AssociationCandidates], optional
        The association candidates. These are objects believed to be associated with a known object.
    association_members : Optional[AssociationMembers], optional
        The observation members of the association candidates.
    max_observations_per_file : Optional[int], optional
        The maximum number of observations per file, by default 100000.

    Returns
    -------
    discovery_ades, association_ades : tuple[List[ADESObservations], List[ADESObservations]]
        The ADESObservations for the discovery and association candidates.
    """
    if discovery_candidates is None and association_candidates is None:
        raise ValueError(
            "At least one of discovery_candidates or association_candidates must be provided."
        )

    if discovery_candidates is not None:
        if discovery_candidate_members is None:
            raise ValueError(
                "discovery_candidate_members must be provided if discovery_candidates is provided."
            )

        if not pc.all(
            pc.is_in(discovery_candidates.trksub, discovery_candidate_members.trksub)
        ).as_py():
            raise ValueError(
                "All trksubs in discovery_candidates must be present in discovery_candidate_members."
            )

        if not pc.all(
            pc.is_in(discovery_candidate_members.trksub, discovery_candidates.trksub)
        ).as_py():
            raise ValueError(
                "All trksubs in discovery_candidate_members must be present in discovery_candidates."
            )

        if not pc.all(
            pc.is_in(discovery_candidate_members.obssubid, source_catalog.id)
        ).as_py():
            raise ValueError(
                "All obssubids in discovery_candidate_members must be present in source_catalog."
            )

    if association_candidates is not None:
        if association_members is None:
            raise ValueError(
                "association_members must be provided if association_candidates is provided."
            )

        if not pc.all(
            pc.is_in(association_candidates.trksub, association_members.trksub)
        ).as_py():
            raise ValueError(
                "All trksubs in association_candidates must be present in association_members."
            )

        if not pc.all(
            pc.is_in(association_members.trksub, association_candidates.trksub)
        ).as_py():
            raise ValueError(
                "All trksubs in association_members must be present in association_candidates."
            )

        if not pc.all(
            pc.is_in(association_members.obssubid, source_catalog.id)
        ).as_py():
            raise ValueError(
                "All obssubids in association_members must be present in source_catalog."
            )

    # Create ADES Observations for discovery candidates
    if discovery_candidates is not None:
        if len(discovery_candidates) == 0:
            discovery_ades = [ADESObservations.empty()]
        else:
            discovery_ades = candidates_to_ades(
                discovery_candidates,
                discovery_candidate_members,
                source_catalog,
                max_observations_per_table=max_observations_per_file,
            )

    else:
        discovery_ades = [ADESObservations.empty()]

    # Create ADES Observations for association candidates
    if association_candidates is not None:
        if len(association_candidates) == 0:
            association_ades = [ADESObservations.empty()]
        else:
            association_ades = candidates_to_ades(
                association_candidates,
                association_members,
                source_catalog,
                max_observations_per_table=max_observations_per_file,
            )

    else:
        association_ades = [ADESObservations.empty()]

    return discovery_ades, association_ades
