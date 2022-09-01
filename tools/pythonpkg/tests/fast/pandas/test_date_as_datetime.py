import pandas as pd
import duckdb
import datetime

def run_checks(df):
    assert type(df['d'][0]) is datetime.date
    assert df['d'][0] == datetime.date(1992, 7, 30)
    assert pd.isnull(df['d'][1])

def test_date_as_datetime():
    con = duckdb.connect()
    con.execute("create table t (d date)")
    con.execute("insert into t values ('1992-07-30'), (NULL)")

    # Connection Methods
    run_checks(con.execute("Select * from t").df(True))
    run_checks(con.execute("Select * from t").fetchdf(True))
    run_checks(con.execute("Select * from t").fetch_df_chunk(True))
    run_checks(con.execute("Select * from t").fetch_df(True))

    # Relation Methods
    rel = con.table('t')
    run_checks(rel.df(True))
    run_checks(rel.to_df(True))

    # Result Methods
    run_checks(rel.query("t_1", "select * from t_1").df(True))
    run_checks(rel.query("t_1", "select * from t_1").fetchdf(True))
    run_checks(rel.query("t_1", "select * from t_1").fetch_df_chunk(True))
    run_checks(rel.query("t_1", "select * from t_1").fetch_df(True))

