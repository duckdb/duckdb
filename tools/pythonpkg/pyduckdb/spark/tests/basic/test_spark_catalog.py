

import pytest

class TestSparkCatalog(object):
	def test_spark_catalog(self, spark):
		# Get metadata from the Catalog
		# List databases
		dbs = spark.catalog.listDatabases()
		print(dbs)

		# Output
		#[Database(name='default', description='default database', 
		#locationUri='file:/Users/admin/.spyder-py3/spark-warehouse')]

		# List Tables
		tbls = spark.catalog.listTables()
		print(tbls)

		#Output
		#[Table(name='sample_hive_table', database='default', description=None, tableType='MANAGED', #isTemporary=False), Table(name='sample_hive_table1', database='default', description=None, #tableType='MANAGED', isTemporary=False), Table(name='sample_hive_table121', database='default', #description=None, tableType='MANAGED', isTemporary=False), Table(name='sample_table', database=None, #description=None, tableType='TEMPORARY', isTemporary=True)]
