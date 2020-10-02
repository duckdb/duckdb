import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import glob
from decimal import Decimal
import datetime


parquet_compression = 'zstd'

outdir = tempfile.mkdtemp()
parquet_folder = os.path.join(outdir, "out.parquet")
nthreads = 8
memory_gb = 10

spark = SparkSession.builder.master("local[%d]" % nthreads).config('spark.sql.parquet.compression.codec', parquet_compression).config("spark.ui.enabled", "false").config("spark.local.dir", outdir).config("spark.driver.memory", "%dg" % memory_gb).config("spark.executor.memory", "%dg" % memory_gb).getOrCreate()
sc = spark.sparkContext

# https://spark.apache.org/docs/latest/sql-reference.html#data-types

dtval = datetime.datetime.strptime('2019-11-26 21:11:42.501', '%Y-%m-%d %H:%M:%S.%f')

someData = [
    (None, None, None, None, None, None, None, None, None, None, None, None),
    (42, 43, 44, 45, 4.6, 4.7, Decimal(4.8), "49", bytearray(b"50"), True, dtval, datetime.date(2020, 1, 10)),
    (-127, -32767, -2147483647, -9223372036854775807, -4.6, -4.7, None, None, None, False, None, None),
    (127, 32767, 2147483647, 9223372036854775807, None, None, None, None, None, None, None, None),
    (None, None, None, None, None, None, None, None, None, None, None, None)
]

someSchema = StructType([
    StructField("byteval" , ByteType(), True),
    StructField("shortval" , ShortType(), True),
    StructField("integerval" , IntegerType(), True),
    StructField("longval" , LongType(), True),
    StructField("floatval" , FloatType(), True),
    StructField("doubleval" , DoubleType(), True),
    StructField("decimalval" , DecimalType(5,2), True),
    StructField("stringval" , StringType(), True),
    StructField("binaryval" , BinaryType(), True),
    StructField("booleanval" , BooleanType(), True),
    StructField("timestampval" , TimestampType(), True),
    StructField("dateval" , DateType(), True)
])

df = spark.createDataFrame(someData, someSchema).repartition(1)

df.write.mode('overwrite').format("parquet").save(parquet_folder)
os.rename(glob.glob(os.path.join(parquet_folder, '*.parquet'))[0], "data-types.parquet" )

import pandas
print(pandas.read_parquet("data-types.parquet"))
