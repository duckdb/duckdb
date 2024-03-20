from duckdb.typing import DuckDBPyType
from typing import List, Tuple, cast
from .types import (
    DataType,
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

_sqltype_to_spark_class = {
    'boolean': BooleanType,
    'utinyint': UnsignedByteType,
    'tinyint': ByteType,
    'usmallint': UnsignedShortType,
    'smallint': ShortType,
    'uinteger': UnsignedIntegerType,
    'integer': IntegerType,
    'ubigint': UnsignedLongType,
    'bigint': LongType,
    'hugeint': HugeIntegerType,
    'uhugeint': UnsignedHugeIntegerType,
    'varchar': StringType,
    'blob': BinaryType,
    'bit': BitstringType,
    'uuid': UUIDType,
    'date': DateType,
    'time': TimeNTZType,
    'time with time zone': TimeType,
    'timestamp': TimestampNTZType,
    'timestamp with time zone': TimestampType,
    'timestamp_ms': TimestampNanosecondNTZType,
    'timestamp_ns': TimestampMilisecondNTZType,
    'timestamp_s': TimestampSecondNTZType,
    'interval': DayTimeIntervalType,
    'list': ArrayType,
    'struct': StructType,
    'map': MapType,
    # union
    # enum
    # null (???)
    'float': FloatType,
    'double': DoubleType,
    'decimal': DecimalType,
}


def convert_nested_type(dtype: DuckDBPyType) -> DataType:
    id = dtype.id
    if id == 'list':
        children = dtype.children
        return ArrayType(convert_type(children[0][1]))
    # TODO: add support for 'union'
    if id == 'struct':
        children: List[Tuple[str, DuckDBPyType]] = dtype.children
        fields = [StructField(x[0], convert_type(x[1])) for x in children]
        return StructType(fields)
    if id == 'map':
        return MapType(convert_type(dtype.key), convert_type(dtype.value))
    raise NotImplementedError


def convert_type(dtype: DuckDBPyType) -> DataType:
    id = dtype.id
    if id in ['list', 'struct', 'map']:
        return convert_nested_type(dtype)
    if id == 'decimal':
        children: List[Tuple[str, DuckDBPyType]] = dtype.children
        precision = cast(int, children[0][1])
        scale = cast(int, children[1][1])
        return DecimalType(precision, scale)
    spark_type = _sqltype_to_spark_class[id]
    return spark_type()


def duckdb_to_spark_schema(names: List[str], types: List[DuckDBPyType]) -> StructType:
    fields = [StructField(name, dtype) for name, dtype in zip(names, [convert_type(x) for x in types])]
    return StructType(fields)
