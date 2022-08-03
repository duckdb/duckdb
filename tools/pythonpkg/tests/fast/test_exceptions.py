from pytest import raises, mark
from typing import Type, Optional
import duckdb
from re import escape
from duckdb import default_connection


@mark.parametrize(
    'exception,query,message',
    [
        (duckdb.ParserException, 'select', 'Parser Error: SELECT clause without selection list'),
        (duckdb.BinderException, 'select unknown', 'Binder Error: Referenced column "unknown" not found in FROM clause!'),
        # (duckdb.Warning, ''),
        # (duckdb.FatalException, ''),
        # (duckdb.InterruptException, ''),
        # (duckdb.PermissionException, ''),
        # (duckdb.SequenceException, ''),
        # (duckdb.StandardException, ''),
        # (duckdb.DataError, ''),
        # (duckdb.OutOfRangeException, ''),
        # (duckdb.CastException, ''),
        # (duckdb.ConversionException, ''),
        # (duckdb.TypeMismatchException, ''),
        # (duckdb.ValueOutOfRangeException, ''),
        # (duckdb.OperationalError, ''),
        # (duckdb.TransactionException, ''),
        # (duckdb.OutOfMemoryException, ''),
        # (duckdb.IOException, ''),
        # (duckdb.SerializationException, ''),
        # (duckdb.IntegrityError, ''),
        # (duckdb.ConstraintException, ''),
        # (duckdb.InternalError, ''),
        # (duckdb.InternalException, ''),
        # (duckdb.ProgrammingError, ''),
        # (duckdb.SyntaxException, ''),
        # (duckdb.InvalidInputException, ''),
        # (duckdb.InvalidTypeException, ''),
        (duckdb.CatalogException, 'DROP VIEW pg_tables', 'Catalog Error: Cannot drop entry "pg_tables" because it is an internal system entry'),
        # (duckdb.NotSupportedError, ''),
        # (duckdb.NotImplementedException, '')
    ]
)
def test_exceptions(exception: Type[duckdb.Error], query: str, message: str) -> None:
    with raises(exception, match=escape(message)):
        default_connection.execute(query)
