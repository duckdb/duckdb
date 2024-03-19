_exported_symbols = []

# Modules
import duckdb.functional as functional
import duckdb.typing as typing
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

# Takes the function to execute on the 'connection'
def create_wrapper(func):
    def _wrapper(*args, **kwargs):
        connection = duckdb.connect(':default:')
        if 'connection' in kwargs:
            connection = kwargs.pop('connection')
        return func(connection, *args, **kwargs)
    return _wrapper

# Takes the name of a DuckDBPyConnection function to wrap (copying signature, docs, etc)
# The 'func' is what gets executed when the function is called
def create_connection_wrapper(name, func):
    # Define a decorator function that forwards attribute lookup to the default connection
    return functools.wraps(getattr(DuckDBPyConnection, name))(create_wrapper(func))

# These are overloaded twice, we define them inside of C++ so pybind can deal with it
EXCLUDED_METHODS = [
    'df',
    'arrow'
]
_exported_symbols.extend(EXCLUDED_METHODS)
from .duckdb import (
    df,
    arrow
)

methods = [method for method in dir(DuckDBPyConnection) if not is_dunder_method(method) and method not in EXCLUDED_METHODS]
for method_name in methods:
    def create_method_wrapper(method_name):
        def call_method(conn, *args, **kwargs):
            return getattr(conn, method_name)(*args, **kwargs)
        return call_method
    wrapper_function = create_connection_wrapper(method_name, create_method_wrapper(method_name))
    globals()[method_name] = wrapper_function  # Define the wrapper function in the module namespace
    _exported_symbols.append(method_name)


# Specialized "wrapper" methods

SPECIAL_METHODS = [
    'project',
    'distinct',
    'write_csv',
    'aggregate',
    'alias',
    'filter',
    'limit',
    'order',
    'query_df'
]

for method_name in SPECIAL_METHODS:
    def create_method_wrapper(name):
        def _closure(name=name):
            mapping = {
                'alias': 'set_alias',
                'query_df': 'query'
            }
            def call_method(con, df, *args, **kwargs):
                if name in mapping:
                    mapped_name = mapping[name]
                else:
                    mapped_name = name
                return getattr(con.from_df(df), mapped_name)(*args, **kwargs)
            return call_method
        return _closure(name)

    wrapper_function = create_wrapper(create_method_wrapper(method_name))
    globals()[method_name] = wrapper_function  # Define the wrapper function in the module namespace
    _exported_symbols.append(method_name)

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
    connect
)

_exported_symbols.extend([
    "connect"
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
