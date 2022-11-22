import duckdb
import pytest

pa = pytest.importorskip('pyarrow')

def test_reader_produce_consume_same_connection():
	con = duckdb.connect(database=':memory:')
	arrow_table = pa.Table.from_pydict({'i':[1, 2, 3, 4], 'j':["one", "two", "three", "four"]})
	con.execute('SELECT * FROM arrow_table WHERE i < 3')
	duckdb_rbr = con.fetch_record_batch()
	with pytest.raises(IOError, match='Query Stream is closed'):
		con.execute('SELECT * FROM duckdb_rbr WHERE i > 1').fetchall()


def test_reader_produce_consume_dif_connection():
	con = duckdb.connect(database=':memory:')
	con_2 = duckdb.connect(database=':memory:')
	arrow_table = pa.Table.from_pydict({'i':[1, 2, 3, 4], 'j':["one", "two", "three", "four"]})
	con.execute('SELECT * FROM arrow_table WHERE i < 3')
	duckdb_rbr = con.fetch_record_batch()
	# con invalidates the duckdb_rbr by executing something else
	con.execute('SELECT 1') 
	with pytest.raises(IOError, match='Query Stream is closed'):
		con_2.execute('SELECT * FROM duckdb_rbr WHERE i > 1').fetchall()

	con.execute('SELECT * FROM arrow_table WHERE i < 3')
	duckdb_rbr = con.fetch_record_batch()
	# this should work
	res = con_2.execute('SELECT * FROM duckdb_rbr WHERE i > 1').fetchall()
	assert res == [(2, 'two')]

def test_reader_produce_consume_same_connection_delete_reader():
	con = duckdb.connect(database=':memory:')
	arrow_table = pa.Table.from_pydict({'i':[1, 2, 3, 4], 'j':["one", "two", "three", "four"]})
	con.execute('SELECT * FROM arrow_table WHERE i < 3')
	duckdb_rbr = con.fetch_record_batch()
	# con invalidates the duckdb_rbr by executing something else
	del duckdb_rbr
	with pytest.raises(duckdb.CatalogException, match='Table with name duckdb_rbr does not exist!'):
		con.execute('SELECT * FROM duckdb_rbr WHERE i > 1').fetchall()
	
