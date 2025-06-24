import pytest

_ = pytest.importorskip("duckdb.experimental.spark")

from spark_namespace import USE_ACTUAL_SPARK
from spark_namespace.sql.catalog import Table, Database, Column


class TestSparkCatalog(object):
    def test_list_databases(self, spark):
        dbs = spark.catalog.listDatabases()
        if USE_ACTUAL_SPARK:
            assert all(isinstance(db, Database) for db in dbs)
        else:
            assert dbs == [
                Database(name='memory', description=None, locationUri=''),
                Database(name='system', description=None, locationUri=''),
                Database(name='temp', description=None, locationUri=''),
            ]

    def test_list_tables(self, spark):
        # empty
        tbls = spark.catalog.listTables()
        assert tbls == []

        if not USE_ACTUAL_SPARK:
            # Skip this if we're using actual Spark because we can't create tables
            # with our setup.
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

    @pytest.mark.skipif(USE_ACTUAL_SPARK, reason="We can't create tables with our Spark test setup")
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
