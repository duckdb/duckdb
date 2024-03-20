import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
pd = pytest.importorskip("pandas")

from duckdb.experimental.spark.sql.types import (
    LongType,
    StructType,
    BooleanType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    Row,
    ArrayType,
    MapType,
)
from duckdb.experimental.spark.sql.functions import col, struct, when
import duckdb
import re
from pandas.testing import assert_frame_equal


@pytest.fixture
def pandasDF(spark):
    data = [['Scott', 50], ['Jeff', 45], ['Thomas', 54], ['Ann', 34]]
    # Create the pandas DataFrame
    df = pd.DataFrame(data, columns=['Name', 'Age'])
    yield df


class TestPandasDataFrame(object):
    def test_pd_conversion_basic(self, spark, pandasDF):
        sparkDF = spark.createDataFrame(pandasDF)
        res = sparkDF.collect()
        sparkDF.show()
        expected = [
            Row(Name='Scott', Age=50),
            Row(Name='Jeff', Age=45),
            Row(Name='Thomas', Age=54),
            Row(Name='Ann', Age=34),
        ]
        assert res == expected

    def test_pd_conversion_schema(self, spark, pandasDF):
        mySchema = StructType([StructField("First Name", StringType(), True), StructField("Age", IntegerType(), True)])
        sparkDF = spark.createDataFrame(pandasDF, schema=mySchema)
        sparkDF.show()
        res = sparkDF.collect()
        expected = "[Row(First Name='Scott', Age=50), Row(First Name='Jeff', Age=45), Row(First Name='Thomas', Age=54), Row(First Name='Ann', Age=34)]"
        assert str(res) == expected

    def test_spark_to_pandas_dataframe(self, spark, pandasDF):
        sparkDF = spark.createDataFrame(pandasDF)
        res = sparkDF.toPandas()
        assert_frame_equal(res, pandasDF)
