from .. import USE_ACTUAL_SPARK

if USE_ACTUAL_SPARK:
    from pyspark.sql import SparkSession
else:
    from duckdb.experimental.spark.sql import SparkSession
