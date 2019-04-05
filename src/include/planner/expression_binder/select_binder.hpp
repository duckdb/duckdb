//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/select_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/unordered_map.hpp"
#include "parser/expression_map.hpp"
#include "planner/expression_binder.hpp"

namespace duckdb {
class AggregateExpression;
class BoundColumnRefExpression;
class WindowExpression;

class BoundSelectNode;

struct BoundGroupInformation {
	expression_map_t<uint32_t> map;
	unordered_map<string, uint32_t> alias_map;
	vector<SQLType> group_types;
};

//! The SELECT binder is responsible for binding an expression within the SELECT clause of a SQL statement
class SelectBinder : public ExpressionBinder {
public:
	SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info);

protected:
	BindResult BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression = false) override;

	bool inside_window;

	BoundSelectNode &node;
	BoundGroupInformation &info;

protected:
	BindResult BindAggregate(AggregateExpression &expr, uint32_t depth);
	BindResult BindWindow(WindowExpression &expr, uint32_t depth);

	int32_t TryBindGroup(ParsedExpression &expr, uint32_t depth);
	BindResult BindGroup(ParsedExpression &expr, uint32_t depth, int32_t group_index);
};

} // namespace duckdb
