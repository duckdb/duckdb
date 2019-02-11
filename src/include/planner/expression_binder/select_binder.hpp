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
	
//! The SELECT binder is responsible for binding an expression within the SELECT clause of a SQL statement
class SelectBinder : public ExpressionBinder {
public:
	SelectBinder(Binder &binder, ClientContext &context, SelectNode& node, expression_map_t<uint32_t>& group_map, bool has_aggregation);
	SelectBinder(Binder &binder, ClientContext &context, SelectNode& node, expression_map_t<uint32_t>& group_map);

	BindResult BindExpression(unique_ptr<Expression> expr) override;

	bool inside_aggregation;
	bool inside_window;
	bool has_aggregation;

	expression_map_t<uint32_t>& group_map;
};

}
