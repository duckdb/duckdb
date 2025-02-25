//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/operator_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

class OperatorBinder : public ExpressionBinder {
	friend class SelectBinder;

public:
	OperatorBinder(Binder &binder, ClientContext &context, ExpressionType operator_type);

protected:
	BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function, idx_t depth) override;

private:
	ExpressionType operator_type;
};

} // namespace duckdb
