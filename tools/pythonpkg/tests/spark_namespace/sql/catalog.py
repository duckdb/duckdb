from .. import USE_ACTUAL_SPARK

if USE_ACTUAL_SPARK:
    from pyspark.sql.catalog import *
else:
    from duckdb.experimental.spark.sql.catalog import *
