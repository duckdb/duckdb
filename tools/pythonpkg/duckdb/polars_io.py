import duckdb
import polars as pl
from typing import Iterator
from polars.io.plugins import register_io_source

def duckdb_source(relation: duckdb.DuckDBPyRelation) -> pl.LazyFrame:
    """
    A polars IO plugin for DuckDB.
    """
    schema = relation.pl(1).schema
    def source_generator(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        relation_final = relation
        if with_columns is not None:
            cols = ",".join(with_columns)
            relation_final = relation_final.project(cols)
        if predicate is not None:
            raise Exception("Not implemented yet")
        if n_rows is not None:
            relation_final = relation_final.limit(n_rows)
        if batch_size is None:
            results = relation_final.fetch_arrow_reader()
        else:
            results = relation_final.fetch_arrow_reader(batch_size)
        while True:
          try:        
            yield pl.from_arrow(results.read_next_batch())
          except StopIteration:
            break
    return register_io_source(source_generator, schema=schema)
