# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'spark' folder.

from typing import (
    cast,
    overload,
    Dict,
    Optional,
    List,
    Tuple,
    Any,
    Union,
    Type,
    TypeVar,
    ClassVar,
    Iterator,
    TYPE_CHECKING,
)
from builtins import tuple
import datetime
import calendar
import time
import math

import duckdb
from duckdb.typing import DuckDBPyType

from ..exception import ContributionsAcceptedError

T = TypeVar("T")
U = TypeVar("U")

__all__ = [
    "DataType",
    "NullType",
    "StringType",
    "BinaryType",
    "UUIDType",
    "BitstringType",
    "BooleanType",
    "DateType",
    "TimestampType",
    "TimestampNTZType",
    "TimestampNanosecondNTZType",
    "TimestampMilisecondNTZType",
    "TimestampSecondNTZType",
    "TimeType",
    "TimeNTZType",
    "DecimalType",
    "DoubleType",
    "FloatType",
    "ByteType",
    "UnsignedByteType",
    "ShortType",
    "UnsignedShortType",
    "IntegerType",
    "UnsignedIntegerType",
    "LongType",
    "UnsignedLongType",
    "HugeIntegerType",
    "DayTimeIntervalType",
    "Row",
    "ArrayType",
    "MapType",
    "StructField",
    "StructType",
]


class DataType:
    """Base class for data types."""

    def __init__(self, duckdb_type):
        self.duckdb_type = duckdb_type

    def __repr__(self) -> str:
        return self.__class__.__name__ + "()"

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    @classmethod
    def typeName(cls) -> str:
        return cls.__name__[:-4].lower()

    def simpleString(self) -> str:
        return self.typeName()

    def jsonValue(self) -> Union[str, Dict[str, Any]]:
        raise ContributionsAcceptedError

    def json(self) -> str:
        raise ContributionsAcceptedError

    def needConversion(self) -> bool:
        """
        Does this type needs conversion between Python object and internal SQL object.

        This is used to avoid the unnecessary conversion for ArrayType/MapType/StructType.
        """
        return False

    def toInternal(self, obj: Any) -> Any:
        """
        Converts a Python object into an internal SQL object.
        """
        return obj

    def fromInternal(self, obj: Any) -> Any:
        """
        Converts an internal SQL object into a native Python object.
        """
        return obj


# This singleton pattern does not work with pickle, you will get
# another object after pickle and unpickle
class DataTypeSingleton(type):
    """Metaclass for DataType"""

    _instances: ClassVar[Dict[Type["DataTypeSingleton"], "DataTypeSingleton"]] = {}

    def __call__(cls: Type[T]) -> T:  # type: ignore[override]
        if cls not in cls._instances:  # type: ignore[attr-defined]
            cls._instances[cls] = super(DataTypeSingleton, cls).__call__()  # type: ignore[misc, attr-defined]
        return cls._instances[cls]  # type: ignore[attr-defined]


class NullType(DataType, metaclass=DataTypeSingleton):
    """Null type.

    The data type representing None, used for the types that cannot be inferred.
    """

    def __init__(self):
        super().__init__(DuckDBPyType("NULL"))

    @classmethod
    def typeName(cls) -> str:
        return "void"


class AtomicType(DataType):
    """An internal type used to represent everything that is not
    null, UDTs, arrays, structs, and maps."""


class NumericType(AtomicType):
    """Numeric data types."""


class IntegralType(NumericType, metaclass=DataTypeSingleton):
    """Integral data types."""


class FractionalType(NumericType):
    """Fractional data types."""


class StringType(AtomicType, metaclass=DataTypeSingleton):
    """String data type."""

    def __init__(self):
        super().__init__(DuckDBPyType("VARCHAR"))


class BitstringType(AtomicType, metaclass=DataTypeSingleton):
    """Bitstring data type."""

    def __init__(self):
        super().__init__(DuckDBPyType("BIT"))


class UUIDType(AtomicType, metaclass=DataTypeSingleton):
    """UUID data type."""

    def __init__(self):
        super().__init__(DuckDBPyType("UUID"))


class BinaryType(AtomicType, metaclass=DataTypeSingleton):
    """Binary (byte array) data type."""

    def __init__(self):
        super().__init__(DuckDBPyType("BLOB"))


class BooleanType(AtomicType, metaclass=DataTypeSingleton):
    """Boolean data type."""

    def __init__(self):
        super().__init__(DuckDBPyType("BOOLEAN"))


class DateType(AtomicType, metaclass=DataTypeSingleton):
    """Date (datetime.date) data type."""

    def __init__(self):
        super().__init__(DuckDBPyType("DATE"))

    EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()

    def needConversion(self) -> bool:
        return True

    def toInternal(self, d: datetime.date) -> int:
        if d is not None:
            return d.toordinal() - self.EPOCH_ORDINAL

    def fromInternal(self, v: int) -> datetime.date:
        if v is not None:
            return datetime.date.fromordinal(v + self.EPOCH_ORDINAL)


class TimestampType(AtomicType, metaclass=DataTypeSingleton):
    """Timestamp (datetime.datetime) data type."""

    def __init__(self):
        super().__init__(DuckDBPyType("TIMESTAMPTZ"))

    @classmethod
    def typeName(cls) -> str:
        return "timestamptz"

    def needConversion(self) -> bool:
        return True

    def toInternal(self, dt: datetime.datetime) -> int:
        if dt is not None:
            seconds = calendar.timegm(dt.utctimetuple()) if dt.tzinfo else time.mktime(dt.timetuple())
            return int(seconds) * 1000000 + dt.microsecond

    def fromInternal(self, ts: int) -> datetime.datetime:
        if ts is not None:
            # using int to avoid precision loss in float
            return datetime.datetime.fromtimestamp(ts // 1000000).replace(microsecond=ts % 1000000)


class TimestampNTZType(AtomicType, metaclass=DataTypeSingleton):
    """Timestamp (datetime.datetime) data type without timezone information with microsecond precision."""

    def __init__(self):
        super().__init__(DuckDBPyType("TIMESTAMP"))

    def needConversion(self) -> bool:
        return True

    @classmethod
    def typeName(cls) -> str:
        return "timestamp"

    def toInternal(self, dt: datetime.datetime) -> int:
        if dt is not None:
            seconds = calendar.timegm(dt.timetuple())
            return int(seconds) * 1000000 + dt.microsecond

    def fromInternal(self, ts: int) -> datetime.datetime:
        if ts is not None:
            # using int to avoid precision loss in float
            return datetime.datetime.utcfromtimestamp(ts // 1000000).replace(microsecond=ts % 1000000)


class TimestampSecondNTZType(AtomicType, metaclass=DataTypeSingleton):
    """Timestamp (datetime.datetime) data type without timezone information with second precision."""

    def __init__(self):
        super().__init__(DuckDBPyType("TIMESTAMP_S"))

    def needConversion(self) -> bool:
        return True

    @classmethod
    def typeName(cls) -> str:
        return "timestamp_s"

    def toInternal(self, dt: datetime.datetime) -> int:
        raise ContributionsAcceptedError

    def fromInternal(self, ts: int) -> datetime.datetime:
        raise ContributionsAcceptedError


class TimestampMilisecondNTZType(AtomicType, metaclass=DataTypeSingleton):
    """Timestamp (datetime.datetime) data type without timezone information with milisecond precision."""

    def __init__(self):
        super().__init__(DuckDBPyType("TIMESTAMP_MS"))

    def needConversion(self) -> bool:
        return True

    @classmethod
    def typeName(cls) -> str:
        return "timestamp_ms"

    def toInternal(self, dt: datetime.datetime) -> int:
        raise ContributionsAcceptedError

    def fromInternal(self, ts: int) -> datetime.datetime:
        raise ContributionsAcceptedError


class TimestampNanosecondNTZType(AtomicType, metaclass=DataTypeSingleton):
    """Timestamp (datetime.datetime) data type without timezone information with nanosecond precision."""

    def __init__(self):
        super().__init__(DuckDBPyType("TIMESTAMP_NS"))

    def needConversion(self) -> bool:
        return True

    @classmethod
    def typeName(cls) -> str:
        return "timestamp_ns"

    def toInternal(self, dt: datetime.datetime) -> int:
        raise ContributionsAcceptedError

    def fromInternal(self, ts: int) -> datetime.datetime:
        raise ContributionsAcceptedError


class DecimalType(FractionalType):
    """Decimal (decimal.Decimal) data type.

    The DecimalType must have fixed precision (the maximum total number of digits)
    and scale (the number of digits on the right of dot). For example, (5, 2) can
    support the value from [-999.99 to 999.99].

    The precision can be up to 38, the scale must be less or equal to precision.

    When creating a DecimalType, the default precision and scale is (10, 0). When inferring
    schema from decimal.Decimal objects, it will be DecimalType(38, 18).

    Parameters
    ----------
    precision : int, optional
        the maximum (i.e. total) number of digits (default: 10)
    scale : int, optional
        the number of digits on right side of dot. (default: 0)
    """

    def __init__(self, precision: int = 10, scale: int = 0):
        super().__init__(duckdb.decimal_type(precision, scale))
        self.precision = precision
        self.scale = scale
        self.hasPrecisionInfo = True  # this is a public API

    def simpleString(self) -> str:
        return "decimal(%d,%d)" % (self.precision, self.scale)

    def __repr__(self) -> str:
        return "DecimalType(%d,%d)" % (self.precision, self.scale)


class DoubleType(FractionalType, metaclass=DataTypeSingleton):
    """Double data type, representing double precision floats."""

    def __init__(self):
        super().__init__(DuckDBPyType("DOUBLE"))


class FloatType(FractionalType, metaclass=DataTypeSingleton):
    """Float data type, representing single precision floats."""

    def __init__(self):
        super().__init__(DuckDBPyType("FLOAT"))


class ByteType(IntegralType):
    """Byte data type, i.e. a signed integer in a single byte."""

    def __init__(self):
        super().__init__(DuckDBPyType("TINYINT"))

    def simpleString(self) -> str:
        return "tinyint"


class UnsignedByteType(IntegralType):
    """Unsigned byte data type, i.e. a unsigned integer in a single byte."""

    def __init__(self):
        super().__init__(DuckDBPyType("UTINYINT"))

    def simpleString(self) -> str:
        return "utinyint"


class ShortType(IntegralType):
    """Short data type, i.e. a signed 16-bit integer."""

    def __init__(self):
        super().__init__(DuckDBPyType("SMALLINT"))

    def simpleString(self) -> str:
        return "smallint"


class UnsignedShortType(IntegralType):
    """Unsigned short data type, i.e. a unsigned 16-bit integer."""

    def __init__(self):
        super().__init__(DuckDBPyType("USMALLINT"))

    def simpleString(self) -> str:
        return "usmallint"


class IntegerType(IntegralType):
    """Int data type, i.e. a signed 32-bit integer."""

    def __init__(self):
        super().__init__(DuckDBPyType("INTEGER"))

    def simpleString(self) -> str:
        return "integer"


class UnsignedIntegerType(IntegralType):
    """Unsigned int data type, i.e. a unsigned 32-bit integer."""

    def __init__(self):
        super().__init__(DuckDBPyType("UINTEGER"))

    def simpleString(self) -> str:
        return "uinteger"


class LongType(IntegralType):
    """Long data type, i.e. a signed 64-bit integer.

    If the values are beyond the range of [-9223372036854775808, 9223372036854775807],
    please use :class:`DecimalType`.
    """

    def __init__(self):
        super().__init__(DuckDBPyType("BIGINT"))

    def simpleString(self) -> str:
        return "bigint"


class UnsignedLongType(IntegralType):
    """Unsigned long data type, i.e. a unsigned 64-bit integer.

    If the values are beyond the range of [0, 18446744073709551615],
    please use :class:`HugeIntegerType`.
    """

    def __init__(self):
        super().__init__(DuckDBPyType("UBIGINT"))

    def simpleString(self) -> str:
        return "ubigint"


class HugeIntegerType(IntegralType):
    """Huge integer data type, i.e. a signed 128-bit integer.

    If the values are beyond the range of [-170141183460469231731687303715884105727, 170141183460469231731687303715884105727],
    please use :class:`DecimalType`.
    """

    def __init__(self):
        super().__init__(DuckDBPyType("HUGEINT"))

    def simpleString(self) -> str:
        return "hugeint"


class TimeType(IntegralType):
    """Time (datetime.time) data type."""

    def __init__(self):
        super().__init__(DuckDBPyType("TIMETZ"))

    def simpleString(self) -> str:
        return "timetz"


class TimeNTZType(IntegralType):
    """Time (datetime.time) data type without timezone information."""

    def __init__(self):
        super().__init__(DuckDBPyType("TIME"))

    def simpleString(self) -> str:
        return "time"


class DayTimeIntervalType(AtomicType):
    """DayTimeIntervalType (datetime.timedelta)."""

    DAY = 0
    HOUR = 1
    MINUTE = 2
    SECOND = 3

    _fields = {
        DAY: "day",
        HOUR: "hour",
        MINUTE: "minute",
        SECOND: "second",
    }

    _inverted_fields = dict(zip(_fields.values(), _fields.keys()))

    def __init__(self, startField: Optional[int] = None, endField: Optional[int] = None):
        super().__init__(DuckDBPyType("INTERVAL"))
        if startField is None and endField is None:
            # Default matched to scala side.
            startField = DayTimeIntervalType.DAY
            endField = DayTimeIntervalType.SECOND
        elif startField is not None and endField is None:
            endField = startField

        fields = DayTimeIntervalType._fields
        if startField not in fields.keys() or endField not in fields.keys():
            raise RuntimeError("interval %s to %s is invalid" % (startField, endField))
        self.startField = cast(int, startField)
        self.endField = cast(int, endField)

    def _str_repr(self) -> str:
        fields = DayTimeIntervalType._fields
        start_field_name = fields[self.startField]
        end_field_name = fields[self.endField]
        if start_field_name == end_field_name:
            return "interval %s" % start_field_name
        else:
            return "interval %s to %s" % (start_field_name, end_field_name)

    simpleString = _str_repr

    def __repr__(self) -> str:
        return "%s(%d, %d)" % (type(self).__name__, self.startField, self.endField)

    def needConversion(self) -> bool:
        return True

    def toInternal(self, dt: datetime.timedelta) -> Optional[int]:
        if dt is not None:
            return (math.floor(dt.total_seconds()) * 1000000) + dt.microseconds

    def fromInternal(self, micros: int) -> Optional[datetime.timedelta]:
        if micros is not None:
            return datetime.timedelta(microseconds=micros)


class ArrayType(DataType):
    """Array data type.

    Parameters
    ----------
    elementType : :class:`DataType`
        :class:`DataType` of each element in the array.
    containsNull : bool, optional
        whether the array can contain null (None) values.

    Examples
    --------
    >>> ArrayType(StringType()) == ArrayType(StringType(), True)
    True
    >>> ArrayType(StringType(), False) == ArrayType(StringType())
    False
    """

    def __init__(self, elementType: DataType, containsNull: bool = True):
        super().__init__(duckdb.list_type(elementType.duckdb_type))
        assert isinstance(elementType, DataType), "elementType %s should be an instance of %s" % (
            elementType,
            DataType,
        )
        self.elementType = elementType
        self.containsNull = containsNull

    def simpleString(self) -> str:
        return "array<%s>" % self.elementType.simpleString()

    def __repr__(self) -> str:
        return "ArrayType(%s, %s)" % (self.elementType, str(self.containsNull))

    def needConversion(self) -> bool:
        return self.elementType.needConversion()

    def toInternal(self, obj: List[Optional[T]]) -> List[Optional[T]]:
        if not self.needConversion():
            return obj
        return obj and [self.elementType.toInternal(v) for v in obj]

    def fromInternal(self, obj: List[Optional[T]]) -> List[Optional[T]]:
        if not self.needConversion():
            return obj
        return obj and [self.elementType.fromInternal(v) for v in obj]


class MapType(DataType):
    """Map data type.

    Parameters
    ----------
    keyType : :class:`DataType`
        :class:`DataType` of the keys in the map.
    valueType : :class:`DataType`
        :class:`DataType` of the values in the map.
    valueContainsNull : bool, optional
        indicates whether values can contain null (None) values.

    Notes
    -----
    Keys in a map data type are not allowed to be null (None).

    Examples
    --------
    >>> (MapType(StringType(), IntegerType())
    ...        == MapType(StringType(), IntegerType(), True))
    True
    >>> (MapType(StringType(), IntegerType(), False)
    ...        == MapType(StringType(), FloatType()))
    False
    """

    def __init__(self, keyType: DataType, valueType: DataType, valueContainsNull: bool = True):
        super().__init__(duckdb.map_type(keyType.duckdb_type, valueType.duckdb_type))
        assert isinstance(keyType, DataType), "keyType %s should be an instance of %s" % (
            keyType,
            DataType,
        )
        assert isinstance(valueType, DataType), "valueType %s should be an instance of %s" % (
            valueType,
            DataType,
        )
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def simpleString(self) -> str:
        return "map<%s,%s>" % (
            self.keyType.simpleString(),
            self.valueType.simpleString(),
        )

    def __repr__(self) -> str:
        return "MapType(%s, %s, %s)" % (
            self.keyType,
            self.valueType,
            str(self.valueContainsNull),
        )

    def needConversion(self) -> bool:
        return self.keyType.needConversion() or self.valueType.needConversion()

    def toInternal(self, obj: Dict[T, Optional[U]]) -> Dict[T, Optional[U]]:
        if not self.needConversion():
            return obj
        return obj and dict((self.keyType.toInternal(k), self.valueType.toInternal(v)) for k, v in obj.items())

    def fromInternal(self, obj: Dict[T, Optional[U]]) -> Dict[T, Optional[U]]:
        if not self.needConversion():
            return obj
        return obj and dict((self.keyType.fromInternal(k), self.valueType.fromInternal(v)) for k, v in obj.items())


class StructField(DataType):
    """A field in :class:`StructType`.

    Parameters
    ----------
    name : str
        name of the field.
    dataType : :class:`DataType`
        :class:`DataType` of the field.
    nullable : bool, optional
        whether the field can be null (None) or not.
    metadata : dict, optional
        a dict from string to simple type that can be toInternald to JSON automatically

    Examples
    --------
    >>> (StructField("f1", StringType(), True)
    ...      == StructField("f1", StringType(), True))
    True
    >>> (StructField("f1", StringType(), True)
    ...      == StructField("f2", StringType(), True))
    False
    """

    def __init__(
        self,
        name: str,
        dataType: DataType,
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dataType.duckdb_type)
        assert isinstance(dataType, DataType), "dataType %s should be an instance of %s" % (
            dataType,
            DataType,
        )
        assert isinstance(name, str), "field name %s should be a string" % (name)
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}

    def simpleString(self) -> str:
        return "%s:%s" % (self.name, self.dataType.simpleString())

    def __repr__(self) -> str:
        return "StructField('%s', %s, %s)" % (
            self.name,
            self.dataType,
            str(self.nullable),
        )

    def needConversion(self) -> bool:
        return self.dataType.needConversion()

    def toInternal(self, obj: T) -> T:
        return self.dataType.toInternal(obj)

    def fromInternal(self, obj: T) -> T:
        return self.dataType.fromInternal(obj)

    def typeName(self) -> str:  # type: ignore[override]
        raise TypeError("StructField does not have typeName. " "Use typeName on its type explicitly instead.")


class StructType(DataType):
    """Struct type, consisting of a list of :class:`StructField`.

    This is the data type representing a :class:`Row`.

    Iterating a :class:`StructType` will iterate over its :class:`StructField`\\s.
    A contained :class:`StructField` can be accessed by its name or position.

    Examples
    --------
    >>> struct1 = StructType([StructField("f1", StringType(), True)])
    >>> struct1["f1"]
    StructField('f1', StringType(), True)
    >>> struct1[0]
    StructField('f1', StringType(), True)

    >>> struct1 = StructType([StructField("f1", StringType(), True)])
    >>> struct2 = StructType([StructField("f1", StringType(), True)])
    >>> struct1 == struct2
    True
    >>> struct1 = StructType([StructField("f1", StringType(), True)])
    >>> struct2 = StructType([StructField("f1", StringType(), True),
    ...     StructField("f2", IntegerType(), False)])
    >>> struct1 == struct2
    False
    """

    def _update_internal_duckdb_type(self):
        self.duckdb_type = duckdb.struct_type(dict(zip(self.names, [x.duckdb_type for x in self.fields])))

    def __init__(self, fields: Optional[List[StructField]] = None):
        if not fields:
            self.fields = []
            self.names = []
        else:
            self.fields = fields
            self.names = [f.name for f in fields]
            assert all(isinstance(f, StructField) for f in fields), "fields should be a list of StructField"
        # Precalculated list of fields that need conversion with fromInternal/toInternal functions
        self._needConversion = [f.needConversion() for f in self]
        self._needSerializeAnyField = any(self._needConversion)
        super().__init__(duckdb.struct_type(dict(zip(self.names, [x.duckdb_type for x in self.fields]))))

    @overload
    def add(
        self,
        field: str,
        data_type: Union[str, DataType],
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "StructType":
        ...

    @overload
    def add(self, field: StructField) -> "StructType":
        ...

    def add(
        self,
        field: Union[str, StructField],
        data_type: Optional[Union[str, DataType]] = None,
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "StructType":
        """
        Construct a :class:`StructType` by adding new elements to it, to define the schema.
        The method accepts either:

            a) A single parameter which is a :class:`StructField` object.
            b) Between 2 and 4 parameters as (name, data_type, nullable (optional),
               metadata(optional). The data_type parameter may be either a String or a
               :class:`DataType` object.

        Parameters
        ----------
        field : str or :class:`StructField`
            Either the name of the field or a :class:`StructField` object
        data_type : :class:`DataType`, optional
            If present, the DataType of the :class:`StructField` to create
        nullable : bool, optional
            Whether the field to add should be nullable (default True)
        metadata : dict, optional
            Any additional metadata (default None)

        Returns
        -------
        :class:`StructType`

        Examples
        --------
        >>> struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        >>> struct2 = StructType([StructField("f1", StringType(), True), \\
        ...     StructField("f2", StringType(), True, None)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType().add(StructField("f1", StringType(), True))
        >>> struct2 = StructType([StructField("f1", StringType(), True)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType().add("f1", "string", True)
        >>> struct2 = StructType([StructField("f1", StringType(), True)])
        >>> struct1 == struct2
        True
        """
        if isinstance(field, StructField):
            self.fields.append(field)
            self.names.append(field.name)
        else:
            if isinstance(field, str) and data_type is None:
                raise ValueError("Must specify DataType if passing name of struct_field to create.")
            else:
                data_type_f = data_type
            self.fields.append(StructField(field, data_type_f, nullable, metadata))
            self.names.append(field)
        # Precalculated list of fields that need conversion with fromInternal/toInternal functions
        self._needConversion = [f.needConversion() for f in self]
        self._needSerializeAnyField = any(self._needConversion)
        self._update_internal_duckdb_type()
        return self

    def __iter__(self) -> Iterator[StructField]:
        """Iterate the fields"""
        return iter(self.fields)

    def __len__(self) -> int:
        """Return the number of fields."""
        return len(self.fields)

    def __getitem__(self, key: Union[str, int]) -> StructField:
        """Access fields by name or slice."""
        if isinstance(key, str):
            for field in self:
                if field.name == key:
                    return field
            raise KeyError("No StructField named {0}".format(key))
        elif isinstance(key, int):
            try:
                return self.fields[key]
            except IndexError:
                raise IndexError("StructType index out of range")
        elif isinstance(key, slice):
            return StructType(self.fields[key])
        else:
            raise TypeError("StructType keys should be strings, integers or slices")

    def simpleString(self) -> str:
        return "struct<%s>" % (",".join(f.simpleString() for f in self))

    def __repr__(self) -> str:
        return "StructType([%s])" % ", ".join(str(field) for field in self)

    def __contains__(self, item: Any) -> bool:
        return item in self.names

    def extract_types_and_names(self) -> Tuple[List[str], List[str]]:
        names = []
        types = []
        for f in self.fields:
            types.append(str(f.dataType.duckdb_type))
            names.append(f.name)
        return (types, names)

    def fieldNames(self) -> List[str]:
        """
        Returns all field names in a list.

        Examples
        --------
        >>> struct = StructType([StructField("f1", StringType(), True)])
        >>> struct.fieldNames()
        ['f1']
        """
        return list(self.names)

    def needConversion(self) -> bool:
        # We need convert Row()/namedtuple into tuple()
        return True

    def toInternal(self, obj: Tuple) -> Tuple:
        if obj is None:
            return

        if self._needSerializeAnyField:
            # Only calling toInternal function for fields that need conversion
            if isinstance(obj, dict):
                return tuple(
                    f.toInternal(obj.get(n)) if c else obj.get(n)
                    for n, f, c in zip(self.names, self.fields, self._needConversion)
                )
            elif isinstance(obj, (tuple, list)):
                return tuple(f.toInternal(v) if c else v for f, v, c in zip(self.fields, obj, self._needConversion))
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(
                    f.toInternal(d.get(n)) if c else d.get(n)
                    for n, f, c in zip(self.names, self.fields, self._needConversion)
                )
            else:
                raise ValueError("Unexpected tuple %r with StructType" % obj)
        else:
            if isinstance(obj, dict):
                return tuple(obj.get(n) for n in self.names)
            elif isinstance(obj, (list, tuple)):
                return tuple(obj)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                return tuple(d.get(n) for n in self.names)
            else:
                raise ValueError("Unexpected tuple %r with StructType" % obj)

    def fromInternal(self, obj: Tuple) -> "Row":
        if obj is None:
            return
        if isinstance(obj, Row):
            # it's already converted by pickler
            return obj

        values: Union[Tuple, List]
        if self._needSerializeAnyField:
            # Only calling fromInternal function for fields that need conversion
            values = [f.fromInternal(v) if c else v for f, v, c in zip(self.fields, obj, self._needConversion)]
        else:
            values = obj
        return _create_row(self.names, values)


class UnionType(DataType):
    def __init__(self):
        raise ContributionsAcceptedError


class UserDefinedType(DataType):
    """User-defined type (UDT).

    .. note:: WARN: Spark Internal Use Only
    """

    def __init__(self):
        raise ContributionsAcceptedError

    @classmethod
    def typeName(cls) -> str:
        return cls.__name__.lower()

    @classmethod
    def sqlType(cls) -> DataType:
        """
        Underlying SQL storage type for this UDT.
        """
        raise NotImplementedError("UDT must implement sqlType().")

    @classmethod
    def module(cls) -> str:
        """
        The Python module of the UDT.
        """
        raise NotImplementedError("UDT must implement module().")

    @classmethod
    def scalaUDT(cls) -> str:
        """
        The class name of the paired Scala UDT (could be '', if there
        is no corresponding one).
        """
        return ""

    def needConversion(self) -> bool:
        return True

    @classmethod
    def _cachedSqlType(cls) -> DataType:
        """
        Cache the sqlType() into class, because it's heavily used in `toInternal`.
        """
        if not hasattr(cls, "_cached_sql_type"):
            cls._cached_sql_type = cls.sqlType()  # type: ignore[attr-defined]
        return cls._cached_sql_type  # type: ignore[attr-defined]

    def toInternal(self, obj: Any) -> Any:
        if obj is not None:
            return self._cachedSqlType().toInternal(self.serialize(obj))

    def fromInternal(self, obj: Any) -> Any:
        v = self._cachedSqlType().fromInternal(obj)
        if v is not None:
            return self.deserialize(v)

    def serialize(self, obj: Any) -> Any:
        """
        Converts a user-type object into a SQL datum.
        """
        raise NotImplementedError("UDT must implement toInternal().")

    def deserialize(self, datum: Any) -> Any:
        """
        Converts a SQL datum into a user-type object.
        """
        raise NotImplementedError("UDT must implement fromInternal().")

    def simpleString(self) -> str:
        return "udt"

    def __eq__(self, other: Any) -> bool:
        return type(self) == type(other)


_atomic_types: List[Type[DataType]] = [
    StringType,
    BinaryType,
    BooleanType,
    DecimalType,
    FloatType,
    DoubleType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    DateType,
    TimestampType,
    TimestampNTZType,
    NullType,
]
_all_atomic_types: Dict[str, Type[DataType]] = dict((t.typeName(), t) for t in _atomic_types)

_complex_types: List[Type[Union[ArrayType, MapType, StructType]]] = [
    ArrayType,
    MapType,
    StructType,
]
_all_complex_types: Dict[str, Type[Union[ArrayType, MapType, StructType]]] = dict(
    (v.typeName(), v) for v in _complex_types
)

import re

_FIXED_DECIMAL = re.compile(r"decimal\(\s*(\d+)\s*,\s*(-?\d+)\s*\)")
_INTERVAL_DAYTIME = re.compile(r"interval (day|hour|minute|second)( to (day|hour|minute|second))?")


def _create_row(fields: Union["Row", List[str]], values: Union[Tuple[Any, ...], List[Any]]) -> "Row":
    row = Row(*values)
    row.__fields__ = fields
    return row


class Row(tuple):

    """
    A row in :class:`DataFrame`.
    The fields in it can be accessed:

    * like attributes (``row.key``)
    * like dictionary values (``row[key]``)

    ``key in row`` will search through row keys.

    Row can be used to create a row object by using named arguments.
    It is not allowed to omit a named argument to represent that the value is
    None or missing. This should be explicitly set to None in this case.

    .. versionchanged:: 3.0.0
        Rows created from named arguments no longer have
        field names sorted alphabetically and will be ordered in the position as
        entered.

    Examples
    --------
    >>> row = Row(name="Alice", age=11)
    >>> row
    Row(name='Alice', age=11)
    >>> row['name'], row['age']
    ('Alice', 11)
    >>> row.name, row.age
    ('Alice', 11)
    >>> 'name' in row
    True
    >>> 'wrong_key' in row
    False

    Row also can be used to create another Row like class, then it
    could be used to create Row objects, such as

    >>> Person = Row("name", "age")
    >>> Person
    <Row('name', 'age')>
    >>> 'name' in Person
    True
    >>> 'wrong_key' in Person
    False
    >>> Person("Alice", 11)
    Row(name='Alice', age=11)

    This form can also be used to create rows as tuple values, i.e. with unnamed
    fields.

    >>> row1 = Row("Alice", 11)
    >>> row2 = Row(name="Alice", age=11)
    >>> row1 == row2
    True
    """

    @overload
    def __new__(cls, *args: str) -> "Row":
        ...

    @overload
    def __new__(cls, **kwargs: Any) -> "Row":
        ...

    def __new__(cls, *args: Optional[str], **kwargs: Optional[Any]) -> "Row":
        if args and kwargs:
            raise ValueError("Can not use both args " "and kwargs to create Row")
        if kwargs:
            # create row objects
            row = tuple.__new__(cls, list(kwargs.values()))
            row.__fields__ = list(kwargs.keys())
            return row
        else:
            # create row class or objects
            return tuple.__new__(cls, args)

    def asDict(self, recursive: bool = False) -> Dict[str, Any]:
        """
        Return as a dict

        Parameters
        ----------
        recursive : bool, optional
            turns the nested Rows to dict (default: False).

        Notes
        -----
        If a row contains duplicate field names, e.g., the rows of a join
        between two :class:`DataFrame` that both have the fields of same names,
        one of the duplicate fields will be selected by ``asDict``. ``__getitem__``
        will also return one of the duplicate fields, however returned value might
        be different to ``asDict``.

        Examples
        --------
        >>> Row(name="Alice", age=11).asDict() == {'name': 'Alice', 'age': 11}
        True
        >>> row = Row(key=1, value=Row(name='a', age=2))
        >>> row.asDict() == {'key': 1, 'value': Row(name='a', age=2)}
        True
        >>> row.asDict(True) == {'key': 1, 'value': {'name': 'a', 'age': 2}}
        True
        """
        if not hasattr(self, "__fields__"):
            raise TypeError("Cannot convert a Row class into dict")

        if recursive:

            def conv(obj: Any) -> Any:
                if isinstance(obj, Row):
                    return obj.asDict(True)
                elif isinstance(obj, list):
                    return [conv(o) for o in obj]
                elif isinstance(obj, dict):
                    return dict((k, conv(v)) for k, v in obj.items())
                else:
                    return obj

            return dict(zip(self.__fields__, (conv(o) for o in self)))
        else:
            return dict(zip(self.__fields__, self))

    def __contains__(self, item: Any) -> bool:
        if hasattr(self, "__fields__"):
            return item in self.__fields__
        else:
            return super(Row, self).__contains__(item)

    # let object acts like class
    def __call__(self, *args: Any) -> "Row":
        """create new Row object"""
        if len(args) > len(self):
            raise ValueError(
                "Can not create Row with fields %s, expected %d values " "but got %s" % (self, len(self), args)
            )
        return _create_row(self, args)

    def __getitem__(self, item: Any) -> Any:
        if isinstance(item, (int, slice)):
            return super(Row, self).__getitem__(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return super(Row, self).__getitem__(idx)
        except IndexError:
            raise KeyError(item)
        except ValueError:
            raise ValueError(item)

    def __getattr__(self, item: str) -> Any:
        if item.startswith("__"):
            raise AttributeError(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return self[idx]
        except IndexError:
            raise AttributeError(item)
        except ValueError:
            raise AttributeError(item)

    def __setattr__(self, key: Any, value: Any) -> None:
        if key != "__fields__":
            raise RuntimeError("Row is read-only")
        self.__dict__[key] = value

    def __reduce__(
        self,
    ) -> Union[str, Tuple[Any, ...]]:
        """Returns a tuple so Python knows how to pickle Row."""
        if hasattr(self, "__fields__"):
            return (_create_row, (self.__fields__, tuple(self)))
        else:
            return tuple.__reduce__(self)

    def __repr__(self) -> str:
        """Printable representation of Row used in Python REPL."""
        if hasattr(self, "__fields__"):
            return "Row(%s)" % ", ".join("%s=%r" % (k, v) for k, v in zip(self.__fields__, tuple(self)))
        else:
            return "<Row(%s)>" % ", ".join("%r" % field for field in self)
