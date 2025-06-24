# simple DB API testcase

import duckdb
import numpy
import pytest
import re
import os
from conftest import NumpyPandas, ArrowPandas


class TestDBConfig(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_default_order(self, duckdb_cursor, pandas):
        df = pandas.DataFrame({'a': [1, 2, 3]})
        con = duckdb.connect(':memory:', config={'default_order': 'desc'})
        result = con.execute('select * from df order by a').fetchall()
        assert result == [(3,), (2,), (1,)]

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_null_order(self, duckdb_cursor, pandas):
        df = pandas.DataFrame({'a': [1, 2, 3, None]})
        con = duckdb.connect(':memory:', config={'default_null_order': 'nulls_last'})
        result = con.execute('select * from df order by a').fetchall()
        assert result == [(1,), (2,), (3,), (None,)]

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_multiple_options(self, duckdb_cursor, pandas):
        df = pandas.DataFrame({'a': [1, 2, 3, None]})
        con = duckdb.connect(':memory:', config={'default_null_order': 'nulls_last', 'default_order': 'desc'})
        result = con.execute('select * from df order by a').fetchall()
        assert result == [(3,), (2,), (1,), (None,)]

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_external_access(self, duckdb_cursor, pandas):
        df = pandas.DataFrame({'a': [1, 2, 3]})
        # this works (replacement scan)
        con_regular = duckdb.connect(':memory:', config={})
        con_regular.execute('select * from df')
        # disable external access: this also disables pandas replacement scans
        con = duckdb.connect(':memory:', config={'enable_external_access': False})
        # this should fail
        query_failed = False
        try:
            con.execute('select * from df').fetchall()
        except:
            query_failed = True
        assert query_failed == True

    def test_extension_setting(self):
        repository = os.environ.get('LOCAL_EXTENSION_REPO')
        if not repository:
            return
        con = duckdb.connect(config={"TimeZone": "UTC", 'autoinstall_extension_repository': repository})
        assert 'UTC' == con.sql("select current_setting('TimeZone')").fetchone()[0]

    def test_unrecognized_option(self, duckdb_cursor):
        success = True
        try:
            con_regular = duckdb.connect(':memory:', config={'thisoptionisprobablynotthere': '42'})
        except:
            success = False
        assert success == False

    def test_incorrect_parameter(self, duckdb_cursor):
        success = True
        try:
            con_regular = duckdb.connect(':memory:', config={'default_null_order': '42'})
        except:
            success = False
        assert success == False

    def test_user_agent_default(self, duckdb_cursor):
        con_regular = duckdb.connect(':memory:')
        regex = re.compile("duckdb/.* python/.*")
        # Expands to: SELECT * FROM pragma_user_agent()
        assert regex.match(con_regular.sql("PRAGMA user_agent").fetchone()[0]) is not None
        custom_user_agent = con_regular.sql("SELECT current_setting('custom_user_agent')").fetchone()
        assert custom_user_agent[0] == ''

    def test_user_agent_custom(self, duckdb_cursor):
        con_regular = duckdb.connect(':memory:', config={'custom_user_agent': 'CUSTOM_STRING'})
        regex = re.compile("duckdb/.* python/.* CUSTOM_STRING")
        assert regex.match(con_regular.sql("PRAGMA user_agent").fetchone()[0]) is not None
        custom_user_agent = con_regular.sql("SELECT current_setting('custom_user_agent')").fetchone()
        assert custom_user_agent[0] == 'CUSTOM_STRING'

    def test_secret_manager_option(self, duckdb_cursor):
        con = duckdb.connect(':memory:', config={'allow_persistent_secrets': False})
        result = con.execute('select count(*) from duckdb_secrets()').fetchall()
        assert result == [(0,)]
