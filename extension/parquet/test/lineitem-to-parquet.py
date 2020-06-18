import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import glob

parquet_compression = 'snappy'

outdir = tempfile.mkdtemp()
parquet_folder = os.path.join(outdir, "out.parquet")
nthreads = 8
memory_gb = 10

spark = SparkSession.builder.master("local[%d]" % nthreads).config('spark.sql.parquet.compression.codec', parquet_compression).config("spark.ui.enabled", "false").config("spark.local.dir", outdir).config("spark.driver.memory", "%dg" % memory_gb).config("spark.executor.memory", "%dg" % memory_gb).getOrCreate()
sc = spark.sparkContext


schema = StructType([
    StructField("l_orderkey",      LongType(),    False),
    StructField("l_partkey",       LongType(),    False),
    StructField("l_suppkey",       LongType(),    False),

    StructField("l_linenumber",    IntegerType(), False),
    StructField("l_quantity",      IntegerType(), False),

    StructField("l_extendedprice", DoubleType(),  False),
    StructField("l_discount",      DoubleType(),  False),
    StructField("l_tax",           DoubleType(),  False),
 
    StructField("l_returnflag",    StringType(),  False),
    StructField("l_linestatus",    StringType(),  False),

    StructField("l_shipdate",      StringType(),  False),
    StructField("l_commitdate",    StringType(),  False),
    StructField("l_receiptdate",   StringType(),  False),

    StructField("l_shipinstruct",  StringType(),  False),
    StructField("l_shipmode",      StringType(),  False),
    StructField("l_comment",       StringType(),  False)])

df = spark.read.format("csv").schema(schema).option("header", "false").option("delimiter", "|").load("lineitem-sf1.tbl.gz").repartition(1)
df.write.mode('overwrite').format("parquet").save(parquet_folder)

os.rename(glob.glob(os.path.join(parquet_folder, '*.parquet'))[0], "lineitem-sf1.%s.parquet" % parquet_compression)
