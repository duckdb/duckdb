import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from duckdb.experimental.spark.sql.catalog import Table, Database, Column


class TestSparkCatalog(object):
    def test_list_databases(self, spark):
        dbs = spark.catalog.listDatabases()
        assert dbs == [
            Database(name='memory', description=None, locationUri=''),
            Database(name='system', description=None, locationUri=''),
            Database(name='temp', description=None, locationUri=''),
        ]

    def test_list_tables(self, spark):
        # empty
        tbls = spark.catalog.listTables()
        assert tbls == []

        spark.sql('create table tbl(a varchar)')
        tbls = spark.catalog.listTables()
        assert tbls == [
            Table(
                name='tbl',
                database='memory',
                description='CREATE TABLE tbl(a VARCHAR);',
                tableType='',
                isTemporary=False,
            )
        ]

    def test_list_columns(self, spark):
        spark.sql('create table tbl(a varchar, b bool)')
        columns = spark.catalog.listColumns('tbl')
        assert columns == [
            Column(name='a', description=None, dataType='VARCHAR', nullable=True, isPartition=False, isBucket=False),
            Column(name='b', description=None, dataType='BOOLEAN', nullable=True, isPartition=False, isBucket=False),
        ]

        # FIXME: should this error instead?
        non_existant_columns = spark.catalog.listColumns('none_existant')
        assert non_existant_columns == []

        spark.sql('create view vw as select * from tbl')
        view_columns = spark.catalog.listColumns('vw')
        assert view_columns == columns
