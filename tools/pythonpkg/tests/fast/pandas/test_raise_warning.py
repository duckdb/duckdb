import duckdb
import pandas as pd
import pytest


def test_9182():
    pd.set_option('future.infer_string', True)

    _con = duckdb.connect(database=':memory:')

    EU = {
        'BELGIUM',
        'BULGARIA',
        'DENMARK',
        'FINLAND',
        'GERMANY',
        'GREECE',
        'HUNGARY',
        'IRELAND',
        'SPAIN',
        'SWEDEN',
        'SWITZERLAND',
    }

    NAM = {'UNITED STATES', 'CANADA'}
    LATAM = {'ARGENTINA', 'BRAZIL', 'COLOMBIA', 'CHILE', 'MEXICO', 'URUGUAY'}
    AMERICAS = NAM | LATAM

    dfs = {
        'EUROPE': pd.DataFrame({'EUROPE': list(EU)}),
        'AMERICAS': pd.DataFrame({'AMERICAS': list(AMERICAS)}),
    }

    with pytest.warns(None) as warning_record:
        for name, df in dfs.items():
            _con.register(name, df)
        assert len(warning_record) == 0

    assert _con.execute("FROM EUROPE ORDER BY ALL").fetchall() == [
        ('BELGIUM',),
        ('BULGARIA',),
        ('DENMARK',),
        ('FINLAND',),
        ('GERMANY',),
        ('GREECE',),
        ('HUNGARY',),
        ('IRELAND',),
        ('SPAIN',),
        ('SWEDEN',),
        ('SWITZERLAND',),
    ]
    assert _con.execute("FROM AMERICAS ORDER BY ALL").fetchall() == [
        ('ARGENTINA',),
        ('BRAZIL',),
        ('CANADA',),
        ('CHILE',),
        ('COLOMBIA',),
        ('MEXICO',),
        ('UNITED STATES',),
        ('URUGUAY',),
    ]
