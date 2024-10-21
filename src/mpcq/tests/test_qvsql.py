import tempfile
from typing import Generator

import pytest
import quivr as qv
import sqlalchemy as sq

from ..qvsql import SQLQuivrTable


class SQLTable(qv.Table, SQLQuivrTable):

    a = qv.Int64Column()
    b = qv.Float64Column()
    c = qv.LargeStringColumn()
    d = qv.BooleanColumn(nullable=True)


@pytest.fixture
def tracking_db() -> Generator[str, None, None]:
    temp_db = tempfile.NamedTemporaryFile(suffix=".db")
    temp_db.close()

    engine = sq.create_engine(f"sqlite:///{temp_db.name}")
    metadata = sq.MetaData()
    sq.Table(
        "test_table",
        metadata,
        sq.Column("a", sq.Integer, primary_key=True),
        sq.Column("b", sq.Float, primary_key=True),
        sq.Column("c", sq.String),
        sq.Column("d", sq.Boolean, nullable=True),
    )

    metadata.create_all(engine)
    yield temp_db.name


@pytest.fixture
def engine(tracking_db) -> Generator[sq.engine.Engine, None, None]:
    engine = sq.create_engine(f"sqlite:///{tracking_db}")
    yield engine
    engine.dispose()


def test_to_from_sql(engine: sq.engine.Engine) -> None:
    # Test that we can save and load a table to and from SQL
    table = SQLTable.from_kwargs(
        a=[1, 2, 3],
        b=[1.1, 2.2, 3.3],
        c=["a", "b", "c"],
        d=[True, False, None],
    )

    table.to_sql(engine, "test_table")
    loaded_table = SQLTable.from_sql(engine, "test_table")

    assert table == loaded_table


def test_to_from_sql_chunked(engine: sq.engine.Engine) -> None:
    # Test that we can save and load a table to and from SQL
    table = SQLTable.from_kwargs(
        a=[1, 2, 3],
        b=[1.1, 2.2, 3.3],
        c=["a", "b", "c"],
        d=[True, False, None],
    )

    table.to_sql(engine, "test_table", chunk_size=1)
    loaded_table = SQLTable.from_sql(engine, "test_table", chunk_size=1)

    assert table == loaded_table


def test_to_sql_insert(engine: sq.engine.Engine) -> None:
    # Test that we can insert a new table into an existing table
    table = SQLTable.from_kwargs(
        a=[1, 2, 3],
        b=[1.1, 2.2, 3.3],
        c=["a", "b", "c"],
        d=[True, False, None],
    )

    table.to_sql(engine, "test_table")

    table2 = SQLTable.from_kwargs(
        a=[4, 5, 6],
        b=[4.4, 5.5, 6.6],
        c=["d", "e", "f"],
        d=[True, False, None],
    )
    table2.to_sql(engine, "test_table")

    expected_table = qv.concatenate([table, table2])
    loaded_table = SQLTable.from_sql(engine, "test_table")
    assert expected_table == loaded_table


def test_to_sql_upsert(engine: sq.engine.Engine) -> None:
    # Test that we can upsert a new table into an existing table
    table = SQLTable.from_kwargs(
        a=[1, 2, 3],
        b=[1.1, 2.2, 3.3],
        c=["a", "b", "c"],
        d=[True, False, None],
    )

    table.to_sql(engine, "test_table")

    table2 = SQLTable.from_kwargs(
        a=[3, 4, 5],
        b=[3.3, 4.4, 5.5],
        c=["c", "d", "e"],
        d=[False, False, None],
    )
    table2.to_sql(engine, "test_table", if_exists="append")

    expected_table = qv.concatenate([table[:2], table2])
    loaded_table = SQLTable.from_sql(engine, "test_table")
    assert expected_table == loaded_table


def test_to_sql_replace(engine: sq.engine.Engine) -> None:
    # Test that we can replace a table with a new table
    table = SQLTable.from_kwargs(
        a=[1, 2, 3],
        b=[1.1, 2.2, 3.3],
        c=["a", "b", "c"],
        d=[True, False, None],
    )

    table.to_sql(engine, "test_table")

    table2 = SQLTable.from_kwargs(
        a=[3, 4, 5],
        b=[3.3, 4.4, 5.5],
        c=["c", "d", "e"],
        d=[False, False, None],
    )
    table2.to_sql(engine, "test_table", if_exists="replace")

    loaded_table = SQLTable.from_sql(engine, "test_table")
    assert table2 == loaded_table


def test_to_sql_insert_fail(engine: sq.engine.Engine) -> None:
    # Test that we can't insert a new table into an existing table
    # when the primary key is violated
    table = SQLTable.from_kwargs(
        a=[1, 2, 3],
        b=[1.1, 2.2, 3.3],
        c=["a", "b", "c"],
        d=[True, False, None],
    )

    table.to_sql(engine, "test_table")

    with pytest.raises(sq.exc.IntegrityError):
        table2 = SQLTable.from_kwargs(
            a=[3, 4, 5],
            b=[3.3, 4.4, 5.5],
            c=["c", "d", "e"],
            d=[False, False, None],
        )
        table2.to_sql(engine, "test_table")
