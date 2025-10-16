import pyarrow as pa
import pyarrow.compute as pc
import quivr as qv

from ..utils import split_into_max_size


def test_split_into_max_size():
    class TestTable(qv.Table):
        id = qv.Int64Column()
        value = qv.Int64Column()

    table = TestTable.from_kwargs(
        id=[i for j in range(10) for i in range(10)],
        value=[i for i in range(100)],
    )
    chunks = list(split_into_max_size(table, "id", 10))
    assert len(chunks) == 10
    for i in range(10):
        assert pc.all(pc.equal(chunks[i].id, i)).as_py()


def test_split_into_max_size_uneven():
    class TestTable(qv.Table):
        id = qv.Int64Column()
        value = qv.Int64Column()

    table = TestTable.from_kwargs(
        id=[1, 2, 2, 3, 3, 3, 4, 4, 4, 4],
        value=[i for i in range(10)],
    )
    chunks = list(split_into_max_size(table, "id", 4))
    assert len(chunks) == 3
    assert pc.all(pc.is_in(chunks[0].id.unique(), pa.array([1, 2]))).as_py()
    assert pc.all(pc.is_in(chunks[1].id.unique(), pa.array([3]))).as_py()
    assert pc.all(pc.is_in(chunks[2].id.unique(), pa.array([4]))).as_py()
