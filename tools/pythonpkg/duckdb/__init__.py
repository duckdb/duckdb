_exported_symbols = []

# Modules
import duckdb.functional as functional
import duckdb.typing as typing
import inspect
import functools

_exported_symbols.extend([
    "typing",
    "functional"
])

# Classes
from .duckdb import (
    DuckDBPyRelation,
    DuckDBPyConnection,
	Statement,
    ExplainType,
    StatementType,
    ExpectedResultType,
    PythonExceptionHandling,
    RenderMode,
    Expression,
    ConstantExpression,
    ColumnExpression,
    StarExpression,
    FunctionExpression,
    CaseExpression,
)
_exported_symbols.extend([
    "DuckDBPyRelation",
    "DuckDBPyConnection",
    "ExplainType",
    "PythonExceptionHandling",
    "Expression",
    "ConstantExpression",
    "ColumnExpression",
    "StarExpression",
    "FunctionExpression",
    "CaseExpression",
])

# ---- Wrap the connection methods

def is_dunder_method(method_name: str) -> bool:
    if len(method_name) < 4:
        return False
    return method_name[:2] == '__' and method_name[:-3:-1] == '__'

def create_connection_wrapper(name):
    # Define a decorator function that forwards attribute lookup to the default connection
    @functools.wraps(getattr(DuckDBPyConnection, name))
    def decorator(*args, **kwargs):
        connection = duckdb.connect(':default:')
        if 'connection' in kwargs:
            connection = kwargs.pop('connection')
        return getattr(connection, name)(*args, **kwargs)
    # Set docstring for the wrapper function
    decorator.__doc__ = getattr(DuckDBPyConnection, name).__doc__
    return decorator

methods = inspect.getmembers_static(
    DuckDBPyConnection,
    predicate=inspect.isfunction
)
methods = [method for method in dir(DuckDBPyConnection) if not is_dunder_method(method)]
for name in methods:
    wrapper_function = create_connection_wrapper(name)
    globals()[name] = wrapper_function  # Define the wrapper function in the module namespace
    _exported_symbols.append(name)

# Enums
from .duckdb import (
    ANALYZE,
    DEFAULT,
    RETURN_NULL,
    STANDARD,
	COLUMNS,
	ROWS
)
_exported_symbols.extend([
    "ANALYZE",
    "DEFAULT",
    "RETURN_NULL",
    "STANDARD"
])


# read-only properties
from .duckdb import (
    __standard_vector_size__,
    __interactive__,
    __jupyter__,
    __version__,
    apilevel,
    comment,
    default_connection,
    identifier,
    keyword,
    numeric_const,
    operator,
    paramstyle,
    string_const,
    threadsafety,
    token_type,
    tokenize
)
_exported_symbols.extend([
    "__standard_vector_size__",
    "__interactive__",
    "__jupyter__",
    "__version__",
    "apilevel",
    "comment",
    "default_connection",
    "identifier",
    "keyword",
    "numeric_const",
    "operator",
    "paramstyle",
    "string_const",
    "threadsafety",
    "token_type",
    "tokenize"
])


from .duckdb import (
    connect,
    #project,
    #aggregate,
    #distinct,
    #limit,
    #query_df,
    #order,
    #alias,
    #write_csv
)

_exported_symbols.extend([
    "connect",
    #"project",
    #"aggregate",
    #"distinct",
    #"limit",
    #"query_df",
    #"order",
    #"alias",
    #"write_csv"
])

# Exceptions
from .duckdb import (
    Error,
    DataError,
    ConversionException,
    OutOfRangeException,
    TypeMismatchException,
    FatalException,
    IntegrityError,
    ConstraintException,
    InternalError,
    InternalException,
    InterruptException,
    NotSupportedError,
    NotImplementedException,
    OperationalError,
    ConnectionException,
    IOException,
    HTTPException,
    OutOfMemoryException,
    SerializationException,
    TransactionException,
    PermissionException,
    ProgrammingError,
    BinderException,
    CatalogException,
    InvalidInputException,
    InvalidTypeException,
    ParserException,
    SyntaxException,
    SequenceException,
    Warning
)
_exported_symbols.extend([
    "Error",
    "DataError",
    "ConversionException",
    "OutOfRangeException",
    "TypeMismatchException",
    "FatalException",
    "IntegrityError",
    "ConstraintException",
    "InternalError",
    "InternalException",
    "InterruptException",
    "NotSupportedError",
    "NotImplementedException",
    "OperationalError",
    "ConnectionException",
    "IOException",
    "HTTPException",
    "OutOfMemoryException",
    "SerializationException",
    "TransactionException",
    "PermissionException",
    "ProgrammingError",
    "BinderException",
    "CatalogException",
    "InvalidInputException",
    "InvalidTypeException",
    "ParserException",
    "SyntaxException",
    "SequenceException",
    "Warning"
])

# Value
from .value.constant import (
    Value,
    NullValue,
    BooleanValue,
    UnsignedBinaryValue,
    UnsignedShortValue,
    UnsignedIntegerValue,
    UnsignedLongValue,
    BinaryValue,
    ShortValue,
    IntegerValue,
    LongValue,
    HugeIntegerValue,
    FloatValue,
    DoubleValue,
    DecimalValue,
    StringValue,
    UUIDValue,
    BitValue,
    BlobValue,
    DateValue,
    IntervalValue,
    TimestampValue,
    TimestampSecondValue,
    TimestampMilisecondValue,
    TimestampNanosecondValue,
    TimestampTimeZoneValue,
    TimeValue,
    TimeTimeZoneValue,
)

_exported_symbols.extend([
    "Value",
    "NullValue",
    "BooleanValue",
    "UnsignedBinaryValue",
    "UnsignedShortValue",
    "UnsignedIntegerValue",
    "UnsignedLongValue",
    "BinaryValue",
    "ShortValue",
    "IntegerValue",
    "LongValue",
    "HugeIntegerValue",
    "FloatValue",
    "DoubleValue",
    "DecimalValue",
    "StringValue",
    "UUIDValue",
    "BitValue",
    "BlobValue",
    "DateValue",
    "IntervalValue",
    "TimestampValue",
    "TimestampSecondValue",
    "TimestampMilisecondValue",
    "TimestampNanosecondValue",
    "TimestampTimeZoneValue",
    "TimeValue",
    "TimeTimeZoneValue",
])

__all__ = _exported_symbols
