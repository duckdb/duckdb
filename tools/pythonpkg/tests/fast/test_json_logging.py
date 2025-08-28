import json

import duckdb
import pytest


def _parse_json_func(error_prefix: str):
    """Helper to check that the error message is indeed parsable json"""

    def parse_func(exception):
        msg = exception.args[0]
        assert msg.startswith(error_prefix)
        json_str = msg.split(error_prefix, 1)[1]
        try:
            json.loads(json_str)
        except:
            return False
        return True

    return parse_func


def test_json_syntax_error():
    conn = duckdb.connect()
    conn.execute("SET errors_as_json='true'")
    with pytest.raises(duckdb.ParserException, match="SYNTAX_ERROR", check=_parse_json_func("Parser Error: ")):
        conn.execute("syntax error")


def test_json_catalog_error():
    conn = duckdb.connect()
    conn.execute("SET errors_as_json='true'")
    with pytest.raises(duckdb.CatalogException, match="MISSING_ENTRY", check=_parse_json_func("Catalog Error: ")):
        conn.execute("SELECT * FROM nonexistent_table")


def test_json_syntax_error_extract_statements():
    conn = duckdb.connect()
    conn.execute("SET errors_as_json='true'")
    with pytest.raises(duckdb.ParserException, match="SYNTAX_ERROR", check=_parse_json_func("Parser Error: ")):
        conn.extract_statements("syntax error")


def test_json_syntax_error_get_table_names():
    conn = duckdb.connect()
    conn.execute("SET errors_as_json='true'")
    with pytest.raises(duckdb.ParserException, match="SYNTAX_ERROR", check=_parse_json_func("Parser Error: ")):
        conn.get_table_names("syntax error")
