import duckdb
import numpy as np
import pytest

try:
    import pyarrow as pa

    can_run = True
except:
    can_run = False


def check_equal(duckdb_conn):
    true_result = duckdb_conn.execute("SELECT * from test").fetchall()
    arrow_result = duckdb_conn.execute("SELECT * from testarrow").fetchall()
    assert arrow_result == true_result


def create_and_register_arrow_table(column_list, duckdb_conn):
    pydict = {name: data for (name, _, data) in column_list}
    arrow_schema = pa.schema([(name, dtype) for (name, dtype, _) in column_list])
    res = pa.Table.from_pydict(pydict, schema=arrow_schema)

    duck_from_arrow = duckdb_conn.from_arrow(res)
    duck_from_arrow.create("testarrow")


def create_and_register_comparison_result(column_list, duckdb_conn):
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

    duckdb_conn.execute(query, inserted_values)


class TestArrowListType(object):
    def test_regular_list(self):
        if not can_run:
            return
        duckdb_conn = duckdb.connect()

        n = 5  # Amount of lists
        generated_size = 3  # Size of each list
        list_size = -1  # Argument passed to `pa._list()`

        data = [np.random.random((generated_size)) for _ in range(n)]
        list_type = pa.list_(pa.float32(), list_size=list_size)

        create_and_register_arrow_table(
            [
                ('a', list_type, data),
            ],
            duckdb_conn,
        )
        create_and_register_comparison_result(
            [
                ('a', 'FLOAT[]', data),
            ],
            duckdb_conn,
        )

        check_equal(duckdb_conn)

    def test_fixedsize_list(self):
        if not can_run:
            return
        duckdb_conn = duckdb.connect()

        n = 5  # Amount of lists
        generated_size = 3  # Size of each list
        list_size = 3  # Argument passed to `pa._list()`

        data = [np.random.random((generated_size)) for _ in range(n)]
        list_type = pa.list_(pa.float32(), list_size=list_size)

        create_and_register_arrow_table(
            [
                ('a', list_type, data),
            ],
            duckdb_conn,
        )
        create_and_register_comparison_result(
            [
                ('a', 'FLOAT[]', data),
            ],
            duckdb_conn,
        )

        check_equal(duckdb_conn)

    @pytest.mark.skipif(not hasattr(pa, 'ListViewArray'), reason='The pyarrow version does not support ListViewArrays')
    def test_list_view(self, duckdb_cursor):
        CHILD_SIZE = 100000
        input = [i for i in range(CHILD_SIZE)]
        offsets = []
        sizes = []
        lists = []
        count = 0
        for i in range(CHILD_SIZE):
            if count >= CHILD_SIZE:
                break
            tmp = i % 4
            if tmp == 0:
                size = 3
            elif tmp == 1:
                size = 1
            elif tmp == 2:
                size = 10
            elif tmp == 3:
                size = 5
            size = min(size, CHILD_SIZE - count)
            sizes.append(size)
            offsets.append(count)
            lists.append(input[count : count + size])
            count += size
        offsets.append(CHILD_SIZE)

        # Create a regular ListArray
        list_arr = pa.ListArray.from_arrays(offsets=offsets, values=input)
        list_tbl = pa.Table.from_arrays([list_arr], ['x'])

        lists = list(reversed(lists))
        # Create a ListViewArray
        offsets = []
        input = []
        remaining = CHILD_SIZE
        for i, size in enumerate(sizes):
            remaining -= size
            offsets.append(remaining)
            input.extend(lists[i])
        assert remaining == 0
        list_view_arr = pa.ListViewArray.from_arrays(offsets=offsets, sizes=sizes, values=input)
        list_view_tbl = pa.Table.from_arrays([list_view_arr], ['x'])

        assert list_view_arr.to_pylist() == list_arr.to_pylist()
        original = duckdb_cursor.query("select * from list_tbl").fetchall()
        view = duckdb_cursor.query("select * from list_view_tbl").fetchall()
        assert original == view
