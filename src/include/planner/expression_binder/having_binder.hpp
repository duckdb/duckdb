//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/having_binder.hpp
//
//	
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {
	
//! The HAVING binder is responsible for binding an expression within the HAVING clause of a SQL statement
class HavingBinder : public ExpressionBinder {
public:
	HavingBinder(Binder &binder, ClientContext &context, SelectNode& node, expression_map_t<uint32_t>& group_map);

	BindResult BindExpression(unique_ptr<Expression> expr) override;
private:
	expression_map_t<uint32_t>& group_map;
};

}
