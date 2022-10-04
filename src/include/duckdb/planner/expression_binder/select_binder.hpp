//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/select_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {
class BoundColumnRefExpression;
class WindowExpression;

class BoundSelectNode;

struct BoundGroupInformation {
	expression_map_t<idx_t> map;
	case_insensitive_map_t<idx_t> alias_map;
};

//! The SELECT binder is responsible for binding an expression within the SELECT clause of a SQL statement
class SelectBinder : public ExpressionBinder {
public:
	SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
	             case_insensitive_map_t<idx_t> alias_map);
	SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info);

	bool BoundAggregates() {
		return bound_aggregate;
	}
	void ResetBindings() {
		this->bound_aggregate = false;
		this->bound_columns.clear();
	}

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry *function, idx_t depth) override;

	BindResult BindUnnest(FunctionExpression &function, idx_t depth) override;

	bool inside_window;
	bool bound_aggregate = false;

	BoundSelectNode &node;
	BoundGroupInformation &info;
	case_insensitive_map_t<idx_t> alias_map;

protected:
	BindResult BindColumnRef(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth);
	BindResult BindGroupingFunction(OperatorExpression &op, idx_t depth) override;
	BindResult BindWindow(WindowExpression &expr, idx_t depth);

	idx_t TryBindGroup(ParsedExpression &expr, idx_t depth);
	BindResult BindGroup(ParsedExpression &expr, idx_t depth, idx_t group_index);
};

} // namespace duckdb
