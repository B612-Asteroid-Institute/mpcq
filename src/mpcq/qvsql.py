from typing import Union

import pyarrow as pa
import quivr as qv
import sqlalchemy as sq


class SQLQuivrTable:

    def to_sql(
        self,
        engine: sq.engine.Engine,
        table: Union[str, sq.Table],
        chunk_size: int = 10000,
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

        Returns
        -------
        None
        """
        if isinstance(table, str):
            metadata = sq.MetaData(bind=engine)
            metadata.reflect(bind=engine)
            table = metadata.tables[table]

        num_rows = len(self)
        arrow_table = self.table

        with engine.begin() as conn:
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

                conn.execute(table.insert().values(data_to_insert))

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
            metadata = sq.MetaData(bind=engine)
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
