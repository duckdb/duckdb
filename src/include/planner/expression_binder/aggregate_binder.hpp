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
class AggregateBinder : public SelectNodeBinder {
public:
	AggregateBinder(Binder &binder, ClientContext &context, SelectNode &node);

	BindResult BindExpression(unique_ptr<Expression> expr, uint32_t depth, bool root_expression = false) override;

	bool BoundColumns() {
		return bound_columns;
	}

private:
	//! Set to true if any columns were successfully bound by this AggregateBinder
	bool bound_columns;
};

} // namespace duckdb
