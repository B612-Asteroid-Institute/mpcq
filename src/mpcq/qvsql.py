from typing import Literal, Union

import pyarrow as pa
import quivr as qv
import sqlalchemy as sq
from sqlalchemy.dialects.sqlite import insert as sqlite_insert


class SQLQuivrTable:

    def to_sql(
        self,
        engine: sq.engine.Engine,
        table: Union[str, sq.Table],
        chunk_size: int = 10000,
        if_exists: Literal["fail", "replace", "append"] = "fail",
    ) -> None:
        """
        Save the pyarrow table to an existing SQL table. If the table does not exist, it will be created.

        Parameters
        ----------
        engine : sqlalchemy.engine.Engine
            SQLAlchemy engine object.
        table : Union[str, sqlalchemy.Table]
            Either a SQLAlchemy Table object or a string of the table name.
        chunk_size : int
            Number of rows to insert at a time.
        if_exists : Literal["fail", "replace", "append"]
            What to do if the table already exists.
                "fail": Raise a ValueError.
                "replace": Drop the table before inserting.
                "append": Insert or upsert rows to the existing table
                    (if the table has a primary key).

        Returns
        -------
        None
        """
        if isinstance(table, str):
            metadata = sq.MetaData()
            metadata.reflect(bind=engine)
            table = metadata.tables[table]

        num_rows = len(self)
        arrow_table = self.table
        primary_keys = {col.name for col in table.primary_key.columns}

        with engine.connect() as conn:

            if if_exists == "replace":
                conn.execute(table.delete())

            for start in range(0, num_rows, chunk_size):

                end = min(start + chunk_size, num_rows)
                chunk = arrow_table.slice(start, end - start)

                data_to_insert = [
                    {
                        table.columns[i].name: chunk.column(i)[row].as_py()
                        for i in range(chunk.num_columns)
                    }
                    for row in range(chunk.num_rows)
                ]

                stmt = sqlite_insert(table)
                if if_exists == "append":
                    stmt = stmt.on_conflict_do_update(
                        index_elements=primary_keys,
                        set_={
                            col: stmt.excluded[col]
                            for col in data_to_insert[0]
                            if col not in primary_keys
                        },
                    )

                conn.execute(stmt.values(data_to_insert))
                conn.commit()

        return

    @classmethod
    def from_sql(
        cls,
        engine: sq.engine.Engine,
        table: Union[str, sq.Table],
        statement: sq.sql.select = None,
        chunk_size: int = 10000,
    ) -> qv.AnyTable:
        """
        Load a SQL table into a quivr table.

        Parameters
        ----------
        engine : sqlalchemy.engine.Engine
            SQLAlchemy engine object.
        table : Union[str, sqlalchemy.Table]
            Either a SQLAlchemy Table object or a string of the table name.
        chunk_size : int
            Number of rows to load at a time.

        Returns
        -------
        qv.AnyTable
            The quivr table.
        """
        # If table is passed as a string, load the table from metadata
        if isinstance(table, str):
            metadata = sq.MetaData()
            metadata.reflect(bind=engine)
            table = metadata.tables[table]

        with engine.connect() as conn:

            # Select all data from the table
            if statement is None:
                stmt = sq.select(table)
            else:
                stmt = statement

            result_proxy = conn.execute(stmt)

            qtable = cls.empty()
            while True:
                chunk = result_proxy.fetchmany(chunk_size)
                if not chunk:
                    break

                data_dict = {col.name: [] for col in table.columns}

                for row in chunk:
                    for col_name, value in row._mapping.items():
                        data_dict[col_name].append(value)

                qtable_i = qtable.from_pyarrow(pa.Table.from_pydict(data_dict))
                qtable = qv.concatenate([qtable, qtable_i])
                if qtable.fragmented():
                    qtable = qv.defragment(qtable)

        return qtable
