//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/group_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {
class ConstantExpression;
class ColumnRefExpression;
struct SelectBindState;

//! The GROUP binder is responsible for binding expressions in the GROUP BY clause
class GroupBinder : public ExpressionBinder {
public:
	GroupBinder(Binder &binder, ClientContext &context, SelectNode &node, TableIndex group_index,
	            SelectBindState &bind_state);

public:
	static void ReplaceSelectRef(SelectNode &node, SelectBindState &bind_state, ProjectionIndex index,
	                             unique_ptr<ParsedExpression> &expr_ptr);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override;

	string UnsupportedAggregateMessage() override;

	bool TryResolveAliasReference(ColumnRefExpression &colref, idx_t depth, bool root_expression, BindResult &result,
	                              unique_ptr<ParsedExpression> &expr_ptr) override;

private:
	SelectNode &node;
	SelectBindState &bind_state;

	TableIndex group_index;
};

} // namespace duckdb
