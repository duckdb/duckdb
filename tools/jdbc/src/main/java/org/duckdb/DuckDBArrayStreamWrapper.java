package org.duckdb;

public interface DuckDBArrayStreamWrapper {
  void exportArrayStream(long arrowArrayStreamPointer);
  void exportSchema(long arrowSchemaPointer);
}
