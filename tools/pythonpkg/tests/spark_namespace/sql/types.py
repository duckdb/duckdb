from .. import USE_ACTUAL_SPARK

if USE_ACTUAL_SPARK:
    from pyspark.sql.types import *
else:
    from duckdb.experimental.spark.sql.types import *
