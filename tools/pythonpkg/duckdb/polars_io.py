import duckdb
import polars as pl
from typing import Iterator, Optional

from polars.io.plugins import register_io_source
from duckdb import SQLExpression
import json
from decimal import Decimal
import datetime

def _predicate_to_expression(predicate: pl.Expr) -> Optional[SQLExpression]:
    """
    Convert a Polars predicate expression to a DuckDB-compatible SQL expression.
    
    Parameters:
        predicate (pl.Expr): A Polars expression (e.g., col("foo") > 5)
    
    Returns:
        SQLExpression: A DuckDB SQL expression string equivalent.
        None: If conversion fails.

    Example:
        >>> _predicate_to_expression(pl.col("foo") > 5)
        SQLExpression("(foo > 5)")
    """
    # Serialize the Polars expression tree to JSON
    tree = json.loads(predicate.meta.serialize(format="json"))
    
    try:
        # Convert the tree to SQL
        sql_filter = _pl_tree_to_sql(tree)
        return SQLExpression(sql_filter)
    except:
        # If the conversion fails, we return None
        return None


def _pl_operation_to_sql(op: str) -> str:
    """
    Map Polars binary operation strings to SQL equivalents.
    
    Example:
        >>> _pl_operation_to_sql("Eq")
        '='
    """
    try:
        return {
            "Lt": "<",
            "LtEq": "<=",
            "Gt": ">",
            "GtEq": ">=",
            "Eq": "=",
            "Modulus": "%",
            "And": "AND",
            "Or": "OR",
        }[op]
    except KeyError:
        raise NotImplementedError(op)


def _pl_tree_to_sql(tree: dict) -> str:
    """
    Recursively convert a Polars expression tree (as JSON) to a SQL string.
    
    Parameters:
        tree (dict): JSON-deserialized expression tree from Polars
    
    Returns:
        str: SQL expression string
    
    Example:
        Input tree:
        {
            "BinaryExpr": {
                "left": { "Column": "foo" },
                "op": "Gt",
                "right": { "Literal": { "Int": 5 } }
            }
        }
        Output: "(foo > 5)"
    """
    [node_type] = tree.keys()
    subtree = tree[node_type]

    if node_type == "BinaryExpr":
        # Binary expressions: left OP right
        return (
                "(" +
                " ".join((
                    _pl_tree_to_sql(subtree['left']),
                    _pl_operation_to_sql(subtree['op']),
                    _pl_tree_to_sql(subtree['right'])
                )) +
                ")"
        )
    if node_type == "Column":
        # A reference to a column name
        return subtree

    if node_type in ("Literal", "Dyn"):
        # Recursively process dynamic or literal values
        return _pl_tree_to_sql(subtree)

    if node_type == "Int":
        # Direct integer literals
        return str(subtree)

    if node_type == "Function":
        # Handle boolean functions like IsNull, IsNotNull
        inputs = subtree["input"]
        func_dict = subtree["function"]

        if "Boolean" in func_dict:
            func = func_dict["Boolean"]
            arg_sql = _pl_tree_to_sql(inputs[0])

            if func == "IsNull":
                return f"({arg_sql} IS NULL)"
            if func == "IsNotNull":
                return f"({arg_sql} IS NOT NULL)"
            raise NotImplementedError(f"Boolean function not supported: {func}")

        raise NotImplementedError(f"Unsupported function type: {func_dict}")

    if node_type == "Scalar":
        # Detect format: old style (dtype/value) or new style (direct type key)
        if "dtype" in subtree and "value" in subtree:
            dtype = str(subtree["dtype"])
            value = subtree["value"]
        else:
            # New style: dtype is the single key in the dict
            dtype = next(iter(subtree.keys()))
            value = subtree

        # Decimal support
        if dtype.startswith("{'Decimal'") or dtype == "Decimal":
            decimal_value = value['Decimal']
            decimal_value = Decimal(decimal_value[0]) / Decimal(10 ** decimal_value[1])
            return str(decimal_value)

        # Datetime with microseconds since epoch
        if dtype.startswith("{'Datetime'") or dtype == "Datetime":
            micros = value['Datetime'][0]
            dt_timestamp = datetime.datetime.fromtimestamp(micros / 1_000_000, tz=datetime.UTC)
            return f"'{str(dt_timestamp)}'::TIMESTAMP"

        # Match simple numeric/boolean types
        if dtype in ("Int8", "Int16", "Int32", "Int64",
                     "UInt8", "UInt16", "UInt32", "UInt64",
                     "Float32", "Float64", "Boolean"):
            return str(value[dtype])

        # Time type
        if dtype == "Time":
            nanoseconds = value["Time"]
            seconds = nanoseconds // 1_000_000_000
            microseconds = (nanoseconds % 1_000_000_000) // 1_000
            dt_time = (datetime.datetime.min + datetime.timedelta(
                seconds=seconds, microseconds=microseconds
            )).time()
            return f"'{dt_time}'::TIME"

        # Date type
        if dtype == "Date":
            days_since_epoch = value["Date"]
            date = datetime.date(1970, 1, 1) + datetime.timedelta(days=days_since_epoch)
            return f"'{date}'::DATE"

        # Binary type
        if dtype == "Binary":
            binary_data = bytes(value["Binary"])
            escaped = ''.join(f'\\x{b:02x}' for b in binary_data)
            return f"'{escaped}'::BLOB"

        # String type
        if dtype == "String" or dtype == "StringOwned":
            # Some new formats may store directly under StringOwned
            string_val = value.get("StringOwned", value.get("String", None))
            return f"'{string_val}'"


        raise NotImplementedError(f"Unsupported scalar type {str(dtype)}, with value {value}")

    raise NotImplementedError(f"Node type: {node_type} is not implemented. {subtree}")

def duckdb_source(relation: duckdb.DuckDBPyRelation, schema: pl.schema.Schema) -> pl.LazyFrame:
    """
    A polars IO plugin for DuckDB.
    """
    def source_generator(
        with_columns: Optional[list[str]],
        predicate: Optional[pl.Expr],
        n_rows: Optional[int],
        batch_size: Optional[int],
    ) -> Iterator[pl.DataFrame]:
        duck_predicate = None
        relation_final = relation
        if with_columns is not None:
            cols = ",".join(with_columns)
            relation_final = relation_final.project(cols)
        if n_rows is not None:
            relation_final = relation_final.limit(n_rows)
        if predicate is not None:
            # We have a predicate, if possible, we push it down to DuckDB
            duck_predicate = _predicate_to_expression(predicate)
        # Try to pushdown filter, if one exists
        if duck_predicate is not None:
            relation_final = relation_final.filter(duck_predicate)
        if batch_size is None:
            results = relation_final.fetch_arrow_reader()
        else:
            results = relation_final.fetch_arrow_reader(batch_size)
        while True:
            try:
                record_batch = results.read_next_batch()
                df = pl.from_arrow(record_batch)
                if predicate is not None and duck_predicate is None:
                    # We have a predicate, but did not manage to push it down, we fallback here
                    yield pl.from_arrow(record_batch).filter(predicate)
                else:
                    yield pl.from_arrow(record_batch)
            except StopIteration:
                break

    return register_io_source(source_generator, schema=schema)
