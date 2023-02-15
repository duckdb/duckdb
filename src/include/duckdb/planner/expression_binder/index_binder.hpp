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

namespace duckdb {
class BoundColumnRefExpression;

//! The IndexBinder is responsible for binding an expression within an index statement
class IndexBinder : public ExpressionBinder {
public:
	IndexBinder(Binder &binder, ClientContext &context, TableCatalogEntry *table = nullptr);

private:
	// both the table and the ref_expr_indexes are only necessary for binding the expressions of an index
	// during WAL replay
	TableCatalogEntry *table;
	std::unordered_map<idx_t, idx_t> ref_expr_indexes;

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;
};

} // namespace duckdb
