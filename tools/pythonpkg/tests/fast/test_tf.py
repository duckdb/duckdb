import duckdb
import pytest


tf = pytest.importorskip('tensorflow')


def test_tf():
    con = duckdb.connect()

    con.execute("create table t( a integer, b integer)")
    con.execute("insert into t values (1,2), (3,4)")

    # Test from connection
    duck_tf = con.execute("select * from t").tf()
    duck_numpy = con.sql("select * from t").fetchnumpy()
    tf.math.equal(duck_tf['a'], tf.convert_to_tensor(duck_numpy['a']))
    tf.math.equal(duck_tf['b'], tf.convert_to_tensor(duck_numpy['b']))

    # Test from relation
    duck_tf = con.sql("select * from t").tf()
    tf.math.equal(duck_tf['a'], tf.convert_to_tensor(duck_numpy['a']))
    tf.math.equal(duck_tf['b'], tf.convert_to_tensor(duck_numpy['b']))

    # Test all Numeric Types
    numeric_types = ['TINYINT', 'SMALLINT', 'BIGINT', 'HUGEINT', 'FLOAT', 'DOUBLE', 'DECIMAL(4,1)', 'UTINYINT']

    for supported_type in numeric_types:
        con = duckdb.connect()
        con.execute(f"create table t( a {supported_type} , b {supported_type})")
        con.execute("insert into t values (1,2), (3,4)")
        duck_tf = con.sql("select * from t").tf()
        duck_numpy = con.sql("select * from t").fetchnumpy()
        tf.math.equal(duck_tf['a'], tf.convert_to_tensor(duck_numpy['a']))
        tf.math.equal(duck_tf['b'], tf.convert_to_tensor(duck_numpy['b']))
