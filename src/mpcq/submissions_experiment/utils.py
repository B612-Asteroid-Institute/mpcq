import hashlib
import uuid
from datetime import datetime
from typing import Dict, Iterator, List, Literal, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import quivr as qv
from adam_core.observations import ADESObservations, SourceCatalog
from adam_core.time import Timestamp


def round_to_nearest_millisecond(t: datetime) -> datetime:
    microseconds = np.ceil(t.microsecond / 1000).astype(int) * 1000
    return t.replace(microsecond=microseconds)


def compute_file_md5(file_path: str) -> str:
    """
    Compute the MD5 hash of a file.

    Parameters
    ----------
    file_path : str
        The path to the file to compute the MD5 hash of.

    Returns
    -------
    str
        The MD5 hash of the file.
    """
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


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

    column_selection = ["trksub", "obssubid"]
    members_table = members.flattened_table().select(column_selection)
    # TODO: THE FOLLOWING JOIN ASSUMES WE WILL NEVER SUBMIT A CANDIDATE ASSOCIATION WITH MULTIPLE DIFFERENT IDS
    members_table = members_table.join(candidates.flattened_table(), "trksub", "trksub")
    members_observations = members_table.join(
        source_catalog.flattened_table(), "obssubid", "id"
    )
    members_observations = members_observations.combine_chunks()

    ades = ADESObservations.from_kwargs(
        permID=(
            members_observations.column("permid")
            if "permid" in members_observations.column_names
            else None
        ),
        provID=(
            members_observations.column("provid")
            if "provid" in members_observations.column_names
            else None
        ),
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
