//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/aggregate_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {

//! The AggregateBinder is responsible for binding aggregate statements extracted from a SELECT clause (by the
//! SelectBinder)
class AggregateBinder : public ExpressionBinder {
public:
	AggregateBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(ParsedExpression &expr, index_t depth, bool root_expression = false) override;
};

} // namespace duckdb
