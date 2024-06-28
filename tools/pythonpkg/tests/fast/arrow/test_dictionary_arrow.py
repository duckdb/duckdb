import duckdb

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
ds = pytest.importorskip("pyarrow.dataset")
np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
import datetime

Timestamp = pd.Timestamp


class TestArrowDictionary(object):
    def test_dictionary(self, duckdb_cursor):
        indices = pa.array([0, 1, 0, 1, 2, 1, 0, 2])
        dictionary = pa.array([10, 100, None])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array], ['a'])
        rel = duckdb_cursor.from_arrow(arrow_table)

        assert rel.execute().fetchall() == [(10,), (100,), (10,), (100,), (None,), (100,), (10,), (None,)]

        # Bigger than Vector Size
        indices_list = [0, 1, 0, 1, 2, 1, 0, 2, 3] * 10000
        indices = pa.array(indices_list)
        dictionary = pa.array([10, 100, None, 999999])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array], ['a'])
        rel = duckdb_cursor.from_arrow(arrow_table)
        result = [(10,), (100,), (10,), (100,), (None,), (100,), (10,), (None,), (999999,)] * 10000
        assert rel.execute().fetchall() == result

        # Table with dictionary and normal array

        arrow_table = pa.Table.from_arrays([dict_array, pa.array(indices_list)], ['a', 'b'])
        rel = duckdb_cursor.from_arrow(arrow_table)
        result = [(10, 0), (100, 1), (10, 0), (100, 1), (None, 2), (100, 1), (10, 0), (None, 2), (999999, 3)] * 10000
        assert rel.execute().fetchall() == result

    def test_dictionary_null_index(self, duckdb_cursor):
        indices = pa.array([None, 1, 0, 1, 2, 1, 0, 2])
        dictionary = pa.array([10, 100, None])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array], ['a'])
        rel = duckdb_cursor.from_arrow(arrow_table)

        assert rel.execute().fetchall() == [(None,), (100,), (10,), (100,), (None,), (100,), (10,), (None,)]

        indices = pa.array([None, 1, None, 1, 2, 1, 0])
        dictionary = pa.array([10, 100, 100])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array], ['a'])
        rel = duckdb_cursor.from_arrow(arrow_table)
        print(rel.execute().fetchall())
        assert rel.execute().fetchall() == [(None,), (100,), (None,), (100,), (100,), (100,), (10,)]

        # Test Big Vector
        indices_list = [None, 1, None, 1, 2, 1, 0]
        indices = pa.array(indices_list * 1000)
        dictionary = pa.array([10, 100, 100])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array], ['a'])
        rel = duckdb_cursor.from_arrow(arrow_table)
        result = [(None,), (100,), (None,), (100,), (100,), (100,), (10,)] * 1000
        assert rel.execute().fetchall() == result

        # Table with dictionary and normal array
        arrow_table = pa.Table.from_arrays([dict_array, indices], ['a', 'b'])
        rel = duckdb_cursor.from_arrow(arrow_table)
        result = [(None, None), (100, 1), (None, None), (100, 1), (100, 2), (100, 1), (10, 0)] * 1000
        assert rel.execute().fetchall() == result

    @pytest.mark.parametrize(
        'element',
        [
            # list
            """
            ['hello'::ENUM('hello', 'bye')]
        """,
            # struct
            """
            {'a': 'hello'::ENUM('hello', 'bye')}
        """,
            # union
            """
            {'a': 'hello'::ENUM('hello', 'bye')}::UNION(a integer, b bool, c struct(a enum('hello', 'bye')))
        """,
            # map (key)
            """
            map {'hello'::ENUM('hello', 'bye') : 'test'}
        """,
            # map (val)
            """
            map {'test': 'hello'::ENUM('hello', 'bye')}
        """,
            # list of struct(enum)
            """
            [{'a': 'hello'::ENUM('hello', 'bye')}]
        """,
            # list of union(enum)
            """
            [{'a': 'hello'::ENUM('hello', 'bye')}::UNION(a integer, b bool, c struct(a enum('hello', 'bye')))]
        """,
            # list of list
            """
            [['hello'::ENUM('hello', 'bye')], [], NULL, ['hello'::ENUM('hello', 'bye'), 'bye'::ENUM('hello', 'bye')]]
        """,
        ],
    )
    @pytest.mark.parametrize(
        'count',
        [
            1,
            10,
            1024,
            # 2048,
            # 2047,
            # 2049,
            # 4000,
            # 4096,
            5000,
        ],
    )
    @pytest.mark.parametrize('query', ["select {} as a from range({})", "select [{} for x in range({})] as a"])
    def test_dictionary_roundtrip(self, query, element, duckdb_cursor, count):
        query = query.format(element, count)
        original_rel = duckdb_cursor.sql(query)
        expected = original_rel.fetchall()
        arrow_res = original_rel.arrow()

        roundtrip_rel = duckdb_cursor.sql('select * from arrow_res')
        actual = roundtrip_rel.fetchall()
        assert expected == actual
        assert original_rel.columns == roundtrip_rel.columns
        # Note: we can't check the types, because originally these are ENUM
        # but because the dictionary of the ENUM can not be known before execution we output VARCHAR instead.

    def test_dictionary_batches(self, duckdb_cursor):
        indices_list = [None, 1, None, 1, 2, 1, 0]
        indices = pa.array(indices_list * 10000)
        dictionary = pa.array([10, 100, 100])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array], ['a'])
        batch_arrow_table = pa.Table.from_batches(arrow_table.to_batches(10))
        rel = duckdb_cursor.from_arrow(batch_arrow_table)
        result = [(None,), (100,), (None,), (100,), (100,), (100,), (10,)] * 10000
        assert rel.execute().fetchall() == result

        # Table with dictionary and normal array
        arrow_table = pa.Table.from_arrays([dict_array, indices], ['a', 'b'])
        batch_arrow_table = pa.Table.from_batches(arrow_table.to_batches(10))
        rel = duckdb_cursor.from_arrow(batch_arrow_table)
        result = [(None, None), (100, 1), (None, None), (100, 1), (100, 2), (100, 1), (10, 0)] * 10000
        assert rel.execute().fetchall() == result

    def test_dictionary_lifetime(self, duckdb_cursor):
        tables = []
        expected = ''
        for i in range(100):
            if i % 3 == 0:
                input = 'ABCD' * 17000
            elif i % 3 == 1:
                input = 'FOOO' * 17000
            else:
                input = 'BARR' * 17000
            expected += input
            array = pa.array(
                input,
                type=pa.dictionary(pa.int16(), pa.string()),
            )
            tables.append(pa.table([array], names=["x"]))
        # All of the tables with different dictionaries are getting merged into one dataset
        # This is testing that our cache is being evicted correctly
        x = ds.dataset(tables)
        res = duckdb_cursor.sql("select * from x").fetchall()
        expected = [(x,) for x in expected]
        assert res == expected

    def test_dictionary_batches_parallel(self, duckdb_cursor):
        duckdb_cursor.execute("PRAGMA threads=4")
        duckdb_cursor.execute("PRAGMA verify_parallelism")

        indices_list = [None, 1, None, 1, 2, 1, 0]
        indices = pa.array(indices_list * 10000)
        dictionary = pa.array([10, 100, 100])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array], ['a'])
        batch_arrow_table = pa.Table.from_batches(arrow_table.to_batches(10))
        rel = duckdb_cursor.from_arrow(batch_arrow_table)
        result = [(None,), (100,), (None,), (100,), (100,), (100,), (10,)] * 10000
        assert rel.execute().fetchall() == result

        # Table with dictionary and normal array
        arrow_table = pa.Table.from_arrays([dict_array, indices], ['a', 'b'])
        batch_arrow_table = pa.Table.from_batches(arrow_table.to_batches(10))
        rel = duckdb_cursor.from_arrow(batch_arrow_table)
        result = [(None, None), (100, 1), (None, None), (100, 1), (100, 2), (100, 1), (10, 0)] * 10000
        assert rel.execute().fetchall() == result

    def test_dictionary_index_types(self, duckdb_cursor):
        indices_list = [None, 1, None, 1, 2, 1, 0]
        dictionary = pa.array([10, 100, 100], type=pa.uint8())
        index_types = []
        index_types.append(pa.array(indices_list * 10000, type=pa.uint8()))
        index_types.append(pa.array(indices_list * 10000, type=pa.uint16()))
        index_types.append(pa.array(indices_list * 10000, type=pa.uint32()))
        index_types.append(pa.array(indices_list * 10000, type=pa.uint64()))
        index_types.append(pa.array(indices_list * 10000, type=pa.int8()))
        index_types.append(pa.array(indices_list * 10000, type=pa.int16()))
        index_types.append(pa.array(indices_list * 10000, type=pa.int32()))
        index_types.append(pa.array(indices_list * 10000, type=pa.int64()))

        for index_type in index_types:
            dict_array = pa.DictionaryArray.from_arrays(index_type, dictionary)
            arrow_table = pa.Table.from_arrays([dict_array], ['a'])
            rel = duckdb_cursor.from_arrow(arrow_table)
            result = [(None,), (100,), (None,), (100,), (100,), (100,), (10,)] * 10000
            assert rel.execute().fetchall() == result

    def test_dictionary_strings(self, duckdb_cursor):
        indices_list = [None, 0, 1, 2, 3, 4, None]
        indices = pa.array(indices_list * 1000)
        dictionary = pa.array(['Matt Daaaaaaaaamon', 'Alec Baldwin', 'Sean Penn', 'Tim Robbins', 'Samuel L. Jackson'])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array], ['a'])
        rel = duckdb_cursor.from_arrow(arrow_table)
        result = [
            (None,),
            ('Matt Daaaaaaaaamon',),
            ('Alec Baldwin',),
            ('Sean Penn',),
            ('Tim Robbins',),
            ('Samuel L. Jackson',),
            (None,),
        ] * 1000
        assert rel.execute().fetchall() == result

    def test_dictionary_timestamps(self, duckdb_cursor):
        indices_list = [None, 0, 1, 2, None]
        indices = pa.array(indices_list * 1000)
        dictionary = pa.array(
            [
                Timestamp(year=2001, month=9, day=25),
                Timestamp(year=2006, month=11, day=14),
                Timestamp(year=2012, month=5, day=15),
                Timestamp(year=2018, month=11, day=2),
            ]
        )
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array], ['a'])
        rel = duckdb_cursor.from_arrow(arrow_table)
        print(rel.execute().fetchall())
        expected = [
            (None,),
            (datetime.datetime(2001, 9, 25, 0, 0),),
            (datetime.datetime(2006, 11, 14, 0, 0),),
            (datetime.datetime(2012, 5, 15, 0, 0),),
            (None,),
        ] * 1000
        result = rel.execute().fetchall()
        assert result == expected
