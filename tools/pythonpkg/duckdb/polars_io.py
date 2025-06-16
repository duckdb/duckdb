import duckdb
import polars as pl
from typing import Iterator
from polars.io.plugins import register_io_source
from duckdb import SQLExpression
import json

# Code to convert a Polars to a DuckDB 
def _predicate_to_expression(predicate: pl.Expr):
    """Convert a polars predicate to a DuckDB expression"""
    # Polars does not seem to have an API to properly consume their expression tree yet
    tree = json.loads(predicate.meta.serialize(format="json"))
    try:
        sql_filter =  _pl_tree_to_sql(tree)
        return SQLExpression(sql_filter)
    except:
        return None

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
        case "Literal":
            return _pl_tree_to_sql(subtree)
        case "Dyn":
            return _pl_tree_to_sql(subtree)
        case "Int":
            return str(subtree)
        case "Scalar":
            match subtree['dtype']:
               case "Int32":
                   return str(subtree['value']['Int32'])
               case "String":
                   return "'{}'".format(subtree['value']['StringOwned'])
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
