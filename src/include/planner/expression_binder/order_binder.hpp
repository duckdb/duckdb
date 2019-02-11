//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/order_binder.hpp
//
//	
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {
	
//! The ORDER binder is responsible for binding an expression within the ORDER BY clause of a SQL statement
class OrderBinder : public ExpressionBinder {
public:
	OrderBinder(Binder &binder, ClientContext &context, SelectNode& node, unordered_map<string, uint32_t>& alias_map, expression_map_t<uint32_t>& projection_map);

	BindResult BindExpression(unique_ptr<Expression> expr, uint32_t depth) override;
private:
	BindResult CreateProjectionReference(Expression &expr, size_t index);
	
	unordered_map<string, uint32_t>& alias_map;
	expression_map_t<uint32_t>& projection_map;
};

}
