

import pytest
from pyduckdb.spark.sql.catalog import Table, Database

class TestSparkCatalog(object):
	def test_list_databases(self, spark):
		dbs = spark.catalog.listDatabases()
		assert dbs == [
			Database(name='memory', description=None, locationUri=''),
			Database(name='system', description=None, locationUri=''),
			Database(name='temp', description=None, locationUri='')
		]

	def test_list_tables(self, spark):
		spark.sql('create table tbl(a varchar)')
		tbls = spark.catalog.listTables()
		assert tbls == [
			Table(name='tbl', database='memory', description='CREATE TABLE tbl(a VARCHAR);', tableType='', isTemporary=False)
		]
