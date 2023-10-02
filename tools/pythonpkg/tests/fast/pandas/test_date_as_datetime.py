import pandas as pd
import duckdb
import datetime
import pytest


def run_checks(df):
    assert type(df['d'][0]) is datetime.date
    assert df['d'][0] == datetime.date(1992, 7, 30)
    assert pd.isnull(df['d'][1])


def test_date_as_datetime():
    con = duckdb.connect()
    con.execute("create table t (d date)")
    con.execute("insert into t values ('1992-07-30'), (NULL)")

    # Connection Methods
    run_checks(con.execute("Select * from t").df(date_as_object=True))
    run_checks(con.execute("Select * from t").fetchdf(date_as_object=True))
    run_checks(con.execute("Select * from t").fetch_df_chunk(date_as_object=True))
    run_checks(con.execute("Select * from t").fetch_df(date_as_object=True))

    # Relation Methods
    rel = con.table('t')
    run_checks(rel.df(date_as_object=True))
    run_checks(rel.to_df(date_as_object=True))

    # Result Methods
    run_checks(rel.query("t_1", "select * from t_1").df(date_as_object=True))
