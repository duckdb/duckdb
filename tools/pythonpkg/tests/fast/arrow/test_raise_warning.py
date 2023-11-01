import duckdb
import pandas as pd
from typing import final
import pytest

pyarrow = pytest.importorskip("pyarrow")


def test_9182():
    pd.set_option('future.infer_string', True)

    _con = duckdb.connect(database=':memory:')

    EU: final = {
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

    NAM: final = {'UNITED STATES', 'CANADA'}
    LATAM: final = {'ARGENTINA', 'BRAZIL', 'COLOMBIA', 'CHILE', 'MEXICO', 'URUGUAY'}
    AMERICAS: final = NAM | LATAM

    dfs = {
        'EUROPE': pd.DataFrame({'EUROPE': list(EU)}),
        'AMERICAS': pd.DataFrame({'AMERICAS': list(AMERICAS)}),
    }

    with pytest.warns(None) as warning_record:
        for name, df in dfs.items():
            _con.register(name, df)
        assert len(warning_record) == 0
