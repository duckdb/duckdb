//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/base_select_binder.hpp
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
	parsed_expression_map_t<idx_t> map;
	case_insensitive_map_t<idx_t> alias_map;
};

//! The BaseSelectBinder is the base binder of the SELECT, HAVING and QUALIFY binders. It can bind aggregates and window
//! functions.
class BaseSelectBinder : public ExpressionBinder {
public:
	BaseSelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
	                 case_insensitive_map_t<idx_t> alias_map);
	BaseSelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info);

	bool BoundAggregates() {
		return bound_aggregate;
	}
	void ResetBindings() {
		this->bound_aggregate = false;
		this->bound_columns.clear();
	}

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function, idx_t depth) override;

	bool inside_window;
	bool bound_aggregate = false;

	BoundSelectNode &node;
	BoundGroupInformation &info;
	case_insensitive_map_t<idx_t> alias_map;

protected:
	BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth);
	BindResult BindGroupingFunction(OperatorExpression &op, idx_t depth) override;
	BindResult BindWindow(WindowExpression &expr, idx_t depth);

	idx_t TryBindGroup(ParsedExpression &expr, idx_t depth);
	BindResult BindGroup(ParsedExpression &expr, idx_t depth, idx_t group_index);

	bool QualifyColumnAlias(const ColumnRefExpression &colref) override;
};

} // namespace duckdb
