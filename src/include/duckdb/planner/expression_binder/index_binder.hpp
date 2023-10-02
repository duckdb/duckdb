//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/index_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {
class BoundColumnRefExpression;

//! The IndexBinder is responsible for binding an expression within an index statement
class IndexBinder : public ExpressionBinder {
public:
	IndexBinder(Binder &binder, ClientContext &context, optional_ptr<TableCatalogEntry> table = nullptr,
	            optional_ptr<CreateIndexInfo> info = nullptr);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;
	string UnsupportedAggregateMessage() override;

private:
	// only for WAL replay
	optional_ptr<TableCatalogEntry> table;
	optional_ptr<CreateIndexInfo> info;
};

} // namespace duckdb
