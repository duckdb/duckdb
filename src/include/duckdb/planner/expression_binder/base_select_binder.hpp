//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/base_select_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class BoundColumnRefExpression;
class WindowExpression;
class BoundSelectNode;
class AggregateFunctionCatalogEntry;
class Binder;
class ClientContext;
class FunctionExpression;
class OperatorExpression;
class ParsedExpression;

struct BoundGroupInformation {
	parsed_expression_map_t<ProjectionIndex> map;
	case_insensitive_map_t<ProjectionIndex> alias_map;
	unordered_map<ProjectionIndex, ProjectionIndex> collated_groups;
};

//! The BaseSelectBinder is the base binder of the SELECT, HAVING and QUALIFY binders. It can bind aggregates and window
//! functions.
class BaseSelectBinder : public ExpressionBinder {
public:
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

protected:
	BindResult BindGroupingFunction(OperatorExpression &op, idx_t depth) override;

	//! Binds a WINDOW expression and returns the result.
	virtual BindResult BindWindow(WindowExpression &expr, idx_t depth);
	virtual BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression);

	ProjectionIndex TryBindGroup(ParsedExpression &expr);
	BindResult BindGroup(ParsedExpression &expr, idx_t depth, ProjectionIndex group_index);
};

} // namespace duckdb
