import datetime
from typing import Iterator

import numpy as np
import pyarrow.compute as pc
import quivr as qv


def round_to_nearest_millisecond(t: datetime.datetime) -> datetime.datetime:
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
