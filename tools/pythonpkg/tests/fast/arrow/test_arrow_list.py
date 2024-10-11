import duckdb
import numpy as np
import pytest

pa = pytest.importorskip("pyarrow")


def check_equal(duckdb_cursor):
    true_result = duckdb_cursor.execute("SELECT * from test").fetchall()
    arrow_result = duckdb_cursor.execute("SELECT * from testarrow").fetchall()
    assert arrow_result == true_result


def create_and_register_arrow_table(column_list, duckdb_cursor):
    pydict = {name: data for (name, _, data) in column_list}
    arrow_schema = pa.schema([(name, dtype) for (name, dtype, _) in column_list])
    res = pa.Table.from_pydict(pydict, schema=arrow_schema)

    duck_from_arrow = duckdb_cursor.from_arrow(res)
    duck_from_arrow.create("testarrow")


def create_and_register_comparison_result(column_list, duckdb_cursor):
    columns = ",".join([f'{name} {dtype}' for (name, dtype, _) in column_list])
    column_amount = len(column_list)
    assert column_amount
    row_amount = len(column_list[0][2])
    inserted_values = []
    for row in range(row_amount):
        for col in range(column_amount):
            inserted_values.append(column_list[col][2][row])
    inserted_values = tuple(inserted_values)

    column_format = ",".join(['?' for _ in range(column_amount)])
    row_format = ",".join([f"({column_format})" for _ in range(row_amount)])
    query = f"""CREATE TABLE test ({columns});
        INSERT INTO test VALUES {row_format};
    """

    duckdb_cursor.execute(query, inserted_values)


class ListGenerationResult:
    def __init__(self, list, list_view):
        self.list = list
        self.list_view = list_view


def generate_list(child_size) -> ListGenerationResult:
    input = [i for i in range(child_size)]
    offsets = []
    sizes = []
    lists = []
    mask = []
    count = 0
    SIZE_MAP = [3, 1, 10, 0, 5, None]
    for i in range(child_size):
        if count >= child_size:
            break
        size = SIZE_MAP[i % len(SIZE_MAP)]
        if size == None:
            mask.append(True)
            size = 0
        else:
            mask.append(False)
        size = min(size, child_size - count)
        sizes.append(size)
        offsets.append(count)
        lists.append(input[count : count + size])
        count += size
    offsets.append(count)

    # Create a regular ListArray
    list_arr = pa.ListArray.from_arrays(offsets=offsets, values=input, mask=pa.array(mask, type=pa.bool_()))

    if not hasattr(pa, 'ListViewArray'):
        return ListGenerationResult(list_arr, None)

    lists = list(reversed(lists))
    # Create a ListViewArray
    offsets = []
    input = []
    remaining = count
    for i, size in enumerate(sizes):
        remaining -= size
        offsets.append(remaining)
        input.extend(lists[i])
    list_view_arr = pa.ListViewArray.from_arrays(
        offsets=offsets, sizes=sizes, values=input, mask=pa.array(mask, type=pa.bool_())
    )
    return ListGenerationResult(list_arr, list_view_arr)


class TestArrowListType(object):
    def test_regular_list(self, duckdb_cursor):
        n = 5  # Amount of lists
        generated_size = 3  # Size of each list
        list_size = -1  # Argument passed to `pa._list()`

        data = [np.random.random((generated_size)) for _ in range(n)]
        list_type = pa.list_(pa.float32(), list_size=list_size)

        create_and_register_arrow_table(
            [
                ('a', list_type, data),
            ],
            duckdb_cursor,
        )
        create_and_register_comparison_result(
            [
                ('a', 'FLOAT[]', data),
            ],
            duckdb_cursor,
        )

        check_equal(duckdb_cursor)

    def test_fixedsize_list(self, duckdb_cursor):
        n = 5  # Amount of lists
        generated_size = 3  # Size of each list
        list_size = 3  # Argument passed to `pa._list()`

        data = [np.random.random((generated_size)) for _ in range(n)]
        list_type = pa.list_(pa.float32(), list_size=list_size)

        create_and_register_arrow_table(
            [
                ('a', list_type, data),
            ],
            duckdb_cursor,
        )
        create_and_register_comparison_result(
            [
                ('a', f'FLOAT[{list_size}]', data),
            ],
            duckdb_cursor,
        )

        check_equal(duckdb_cursor)

    @pytest.mark.skipif(not hasattr(pa, 'ListViewArray'), reason='The pyarrow version does not support ListViewArrays')
    @pytest.mark.parametrize('child_size', [100000])
    def test_list_view(self, duckdb_cursor, child_size):
        res = generate_list(child_size)

        list_tbl = pa.Table.from_arrays([res.list], ['x'])
        list_view_tbl = pa.Table.from_arrays([res.list_view], ['x'])

        assert res.list_view.to_pylist() == res.list.to_pylist()
        original = duckdb_cursor.query("select * from list_tbl").fetchall()
        view = duckdb_cursor.query("select * from list_view_tbl").fetchall()
        assert original == view
