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

	bool TryResolveAliasReference(ColumnRefExpression &colref, idx_t depth, bool root_expression, BindResult &result,
	                              unique_ptr<ParsedExpression> &expr_ptr) override;

	bool DoesColumnAliasExist(const ColumnRefExpression &colref) override;

protected:
	BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function, idx_t depth) override;
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override;
};

} // namespace duckdb
