//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/table_column_origin.hpp
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

struct TableColumnOrigin {
	reference<TableCatalogEntry> table;
	ColumnIndex column;
};

optional_idx GetDirectReferenceIndex(const Expression &expression, LogicalOperator &input);
optional_idx GetInvertibleReferenceIndex(const Expression &expression, LogicalOperator &input);
//! Without filtering, only direct projections of a complete scan preserve the column's value population.
optional<TableColumnOrigin> GetTableColumnOrigin(LogicalOperator &op, idx_t output_idx, bool allow_filtered = false);
optional<TableColumnOrigin> GetTableColumnOrigin(LogicalOperator &op, const Expression &expression,
                                                 bool allow_filtered = false);
bool SameOrigin(const TableColumnOrigin &left, const TableColumnOrigin &right);

} // namespace duckdb
