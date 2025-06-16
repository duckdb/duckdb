import duckdb
import polars as pl
from typing import Iterator
from polars.io.plugins import register_io_source
from duckdb import SQLExpression
import json
from decimal import Decimal

# Code to convert a Polars to a DuckDB 
def _predicate_to_expression(predicate: pl.Expr):
    """Convert a polars predicate to a DuckDB expression"""
    # Polars does not seem to have an API to properly consume their expression tree yet
    tree = json.loads(predicate.meta.serialize(format="json"))
    # try:
    sql_filter =  _pl_tree_to_sql(tree)
    return SQLExpression(sql_filter)
    # except:
    #     return None

def _pl_operation_to_sql(op: str) -> str:
    """Translate a polars operation string to SQL"""
    match op:
        case "Lt":
            return "<"
        case "LtEq":
            return "<="
        case "Gt":
            return ">"
        case "GtEq":
            return ">="
        case "Eq":
            return "="
        case "Modulus":
            return "%"
        case "And":
            return "AND"
        case "Or":
            return "OR"
        case _:
            raise NotImplementedError(op)

def _pl_tree_to_sql(tree: dict):
    """Convert a polars expression tree to a SQL statement"""
    [node_type] = tree.keys()
    subtree = tree[node_type]
    match node_type:
        case "BinaryExpr":
            return ("("
             + " ".join((_pl_tree_to_sql(subtree['left']), _pl_operation_to_sql(subtree['op']), _pl_tree_to_sql(subtree['right'])))
             + ")")
        case "Column":
            return subtree
        case "Literal" | "Dyn":
            return _pl_tree_to_sql(subtree)
        case "Int":
            return str(subtree)
        case "Function":
            inputs = subtree["input"]
            func_dict = subtree["function"]
            if "Boolean" in func_dict:
                func = func_dict["Boolean"]
                arg_sql = _pl_tree_to_sql(inputs[0])
                match func:
                    case "IsNull":
                        return f"({arg_sql} IS NULL)"
                    case "IsNotNull":
                        return f"({arg_sql} IS NOT NULL)"
                    case _:
                        raise NotImplementedError(f"Boolean function not supported: {func}")
            else:
                raise NotImplementedError(f"Unsupported function type: {func_dict}")
                     
        case "Scalar":
            dtype = subtree["dtype"]
            value = subtree["value"]
            if str(dtype).startswith("{'Decimal'"):
                decimal_value = value['Decimal']
                decimal_value = Decimal(decimal_value[0]) / Decimal(10 ** decimal_value[1])
                return str(decimal_value)
            match dtype:
                case "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64"|"Float32"|"Float64":
                    return str(value[str(dtype)])
                case "String":
                    return "'{}'".format(value['StringOwned'])
                case _:
                    raise NotImplementedError(f"Unsupported scalar type {str(dtype)}, with value {value}")
        case _:
            raise NotImplementedError(node_type)

def duckdb_source(relation: duckdb.DuckDBPyRelation, schema: pl.schema.Schema) -> pl.LazyFrame:
    """
    A polars IO plugin for DuckDB.
    """
    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        duck_predicate = None
        relation_final = relation
        if with_columns is not None:
            cols = ",".join(with_columns)
            relation_final = relation_final.project(cols)
        if n_rows is not None:
            relation_final = relation_final.limit(n_rows)
        if predicate is not None:
            print(predicate)
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
