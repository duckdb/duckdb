//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/try_operator_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

//! This binder is used for the TRY expression
class TryOperatorBinder : public ExpressionBinder {
	friend class SelectBinder;

public:
	TryOperatorBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function, idx_t depth) override;
};

} // namespace duckdb
