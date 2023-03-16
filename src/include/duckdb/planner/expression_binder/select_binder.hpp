//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/select_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder/base_select_binder.hpp"

namespace duckdb {

//! The SELECT binder is responsible for binding an expression within the SELECT clause of a SQL statement
class SelectBinder : public BaseSelectBinder {
public:
	SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
	             case_insensitive_map_t<idx_t> alias_map);
	SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info);

	bool HasExpandedExpressions() {
		return !expanded_expressions.empty();
	}
	vector<unique_ptr<Expression>> &ExpandedExpressions() {
		return expanded_expressions;
	}

protected:
	BindResult BindUnnest(FunctionExpression &function, idx_t depth, bool root_expression) override;

	idx_t unnest_level = 0;
	vector<unique_ptr<Expression>> expanded_expressions;
};

} // namespace duckdb
