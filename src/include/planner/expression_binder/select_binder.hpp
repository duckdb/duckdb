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
class AggregateExpression;
class BoundColumnRefExpression;
class WindowExpression;

class BoundSelectNode;

//! The SELECT binder is responsible for binding an expression within the SELECT clause of a SQL statement
class SelectBinder : public ExpressionBinder {
public:
	SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, expression_map_t<uint32_t> &group_map,
	             unordered_map<string, uint32_t> &group_alias_map);
protected:
	BindResult BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression = false) override;

	bool inside_window;

	BoundSelectNode &node;
	expression_map_t<uint32_t> &group_map;
	unordered_map<string, uint32_t> &group_alias_map;
protected:
	BindResult BindAggregate(AggregateExpression &expr, uint32_t depth);
	BindResult BindWindow(WindowExpression &expr, uint32_t depth);

	unique_ptr<Expression> TryBindGroup(ParsedExpression &expr, uint32_t depth);
};

} // namespace duckdb
