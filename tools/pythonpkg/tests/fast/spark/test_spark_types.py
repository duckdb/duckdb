import pytest

_ = pytest.importorskip("duckdb.experimental.spark")
from duckdb.experimental.spark.sql.types import Row
from duckdb.experimental.spark.sql.types import (
    StringType,
    BinaryType,
    BitstringType,
    UUIDType,
    BooleanType,
    DateType,
    TimestampType,
    TimestampNTZType,
    TimeType,
    TimeNTZType,
    TimestampNanosecondNTZType,
    TimestampMilisecondNTZType,
    TimestampSecondNTZType,
    DecimalType,
    DoubleType,
    FloatType,
    ByteType,
    UnsignedByteType,
    ShortType,
    UnsignedShortType,
    IntegerType,
    UnsignedIntegerType,
    LongType,
    UnsignedLongType,
    HugeIntegerType,
    UnsignedHugeIntegerType,
    DayTimeIntervalType,
    ArrayType,
    MapType,
    StructField,
    StructType,
)


class TestTypes(object):
    def test_all_types_schema(self, spark):
        # Create DataFrame
        df = spark.sql(
            """
			select * EXCLUDE (
				small_enum,
				medium_enum,
				large_enum,
				'union',
				fixed_int_array, 
				fixed_varchar_array, 
				fixed_nested_int_array,
            	fixed_nested_varchar_array, 
            	fixed_struct_array, 
            	struct_of_fixed_array, 
            	fixed_array_of_int_list,
                list_of_fixed_int_array
			) from test_all_types()
		"""
        )
        schema = df.schema
        assert schema == StructType(
            [
                StructField('bool', BooleanType(), True),
                StructField('tinyint', ByteType(), True),
                StructField('smallint', ShortType(), True),
                StructField('int', IntegerType(), True),
                StructField('bigint', LongType(), True),
                StructField('hugeint', HugeIntegerType(), True),
                StructField('uhugeint', UnsignedHugeIntegerType(), True),
                StructField('utinyint', UnsignedByteType(), True),
                StructField('usmallint', UnsignedShortType(), True),
                StructField('uint', UnsignedIntegerType(), True),
                StructField('ubigint', UnsignedLongType(), True),
                StructField('date', DateType(), True),
                StructField('time', TimeNTZType(), True),
                StructField('timestamp', TimestampNTZType(), True),
                StructField('timestamp_s', TimestampSecondNTZType(), True),
                StructField('timestamp_ms', TimestampNanosecondNTZType(), True),
                StructField('timestamp_ns', TimestampMilisecondNTZType(), True),
                StructField('time_tz', TimeType(), True),
                StructField('timestamp_tz', TimestampType(), True),
                StructField('float', FloatType(), True),
                StructField('double', DoubleType(), True),
                StructField('dec_4_1', DecimalType(4, 1), True),
                StructField('dec_9_4', DecimalType(9, 4), True),
                StructField('dec_18_6', DecimalType(18, 6), True),
                StructField('dec38_10', DecimalType(38, 10), True),
                StructField('uuid', UUIDType(), True),
                StructField('interval', DayTimeIntervalType(0, 3), True),
                StructField('varchar', StringType(), True),
                StructField('blob', BinaryType(), True),
                StructField('bit', BitstringType(), True),
                StructField('int_array', ArrayType(IntegerType(), True), True),
                StructField('double_array', ArrayType(DoubleType(), True), True),
                StructField('date_array', ArrayType(DateType(), True), True),
                StructField('timestamp_array', ArrayType(TimestampNTZType(), True), True),
                StructField('timestamptz_array', ArrayType(TimestampType(), True), True),
                StructField('varchar_array', ArrayType(StringType(), True), True),
                StructField('nested_int_array', ArrayType(ArrayType(IntegerType(), True), True), True),
                StructField(
                    'struct',
                    StructType([StructField('a', IntegerType(), True), StructField('b', StringType(), True)]),
                    True,
                ),
                StructField(
                    'struct_of_arrays',
                    StructType(
                        [
                            StructField('a', ArrayType(IntegerType(), True), True),
                            StructField('b', ArrayType(StringType(), True), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    'array_of_structs',
                    ArrayType(
                        StructType([StructField('a', IntegerType(), True), StructField('b', StringType(), True)]), True
                    ),
                    True,
                ),
                StructField('map', MapType(StringType(), StringType(), True), True),
            ]
        )
