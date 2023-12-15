//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/aggregate_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

//! The AggregateBinder is responsible for binding aggregate statements extracted from a SELECT clause (by the
//! SelectBinder)
class AggregateBinder : public ExpressionBinder {
	friend class SelectBinder;

public:
	AggregateBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;
};

} // namespace duckdb
