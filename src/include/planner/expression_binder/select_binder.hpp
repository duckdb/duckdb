//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/select_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {
class BoundColumnRefExpression;

//! The SELECT binder is responsible for binding an expression within the SELECT clause of a SQL statement
class SelectBinder : public SelectNodeBinder {
public:
	SelectBinder(Binder &binder, ClientContext &context, SelectNode &node, expression_map_t<uint32_t> &group_map,
	             unordered_map<string, uint32_t> &group_alias_map);

	BindResult BindExpression(unique_ptr<Expression> expr, uint32_t depth, bool root_expression = false) override;

	bool inside_window;

	expression_map_t<uint32_t> &group_map;
	unordered_map<string, uint32_t> &group_alias_map;
	vector<string> bound_columns;

protected:
	BindResult BindWindow(unique_ptr<Expression> expr, uint32_t depth);
	BindResult BindColumnRef(unique_ptr<Expression> expr, uint32_t depth);
	BindResult BindAggregate(unique_ptr<Expression> expr, uint32_t depth);
	unique_ptr<Expression> TryBindGroup(Expression *expr, uint32_t depth);
};

} // namespace duckdb
