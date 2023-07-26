import duckdb
import pytest


torch = pytest.importorskip('torch')


def test_pytorch():
    con = duckdb.connect()

    con.execute("create table t( a integer, b integer)")
    con.execute("insert into t values (1,2), (3,4)")

    # Test from connection
    duck_torch = con.execute("select * from t").torch()
    duck_numpy = con.sql("select * from t").fetchnumpy()
    torch.equal(duck_torch['a'], torch.tensor(duck_numpy['a']))
    torch.equal(duck_torch['b'], torch.tensor(duck_numpy['b']))

    # Test from relation
    duck_torch = con.sql("select * from t").torch()
    torch.equal(duck_torch['a'], torch.tensor(duck_numpy['a']))
    torch.equal(duck_torch['b'], torch.tensor(duck_numpy['b']))

    # Test all Numeric Types
    numeric_types = ['TINYINT', 'SMALLINT', 'BIGINT', 'HUGEINT', 'FLOAT', 'DOUBLE', 'DECIMAL(4,1)', 'UTINYINT']

    for supported_type in numeric_types:
        con = duckdb.connect()
        con.execute(f"create table t( a {supported_type} , b {supported_type})")
        con.execute("insert into t values (1,2), (3,4)")
        duck_torch = con.sql("select * from t").torch()
        duck_numpy = con.sql("select * from t").fetchnumpy()
        torch.equal(duck_torch['a'], torch.tensor(duck_numpy['a']))
        torch.equal(duck_torch['b'], torch.tensor(duck_numpy['b']))

    with pytest.raises(TypeError, match="can't convert"):
        con = duckdb.connect()
        con.execute(f"create table t( a UINTEGER)")
        duck_torch = con.sql("select * from t").torch()
