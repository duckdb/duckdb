from .. import DuckDBPyConnection

SQLNULL: DuckDBPyType
BOOLEAN: DuckDBPyType
TINYINT: DuckDBPyType
UTINYINT: DuckDBPyType
SMALLINT: DuckDBPyType
USMALLINT: DuckDBPyType
INTEGER: DuckDBPyType
UINTEGER: DuckDBPyType
BIGINT: DuckDBPyType
UBIGINT: DuckDBPyType
HUGEINT: DuckDBPyType
UUID: DuckDBPyType
FLOAT: DuckDBPyType
DOUBLE: DuckDBPyType
DATE: DuckDBPyType
TIMESTAMP: DuckDBPyType
TIMESTAMP_MS: DuckDBPyType
TIMESTAMP_NS: DuckDBPyType
TIMESTAMP_S: DuckDBPyType
TIME: DuckDBPyType
TIME_TZ: DuckDBPyType
TIMESTAMP_TZ: DuckDBPyType
VARCHAR: DuckDBPyType
BLOB: DuckDBPyType
BIT: DuckDBPyType
INTERVAL: DuckDBPyType

class DuckDBPyType:
    def __init__(self, type_str: str, connection: DuckDBPyConnection = ...) -> None: ...
    def __repr__(self) -> str: ...
    def __eq__(self, other) -> bool: ...
    def __getattr__(self, name: str): DuckDBPyType
    def __getitem__(self, name: str): DuckDBPyType
