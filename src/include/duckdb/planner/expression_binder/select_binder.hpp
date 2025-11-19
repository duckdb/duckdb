//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/select_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder/base_select_binder.hpp"

namespace duckdb {

//! The SELECT binder is responsible for binding an expression within the SELECT clause of a SQL statement
class SelectBinder : public BaseSelectBinder {
public:
	SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info);

	bool TryResolveAliasReference(ColumnRefExpression &colref, idx_t depth, bool root_expression,
	                              BindResult &result, unique_ptr<ParsedExpression> &expr_ptr) override;
	bool DoesColumnAliasExist(const ColumnRefExpression &colref) override;

protected:
	void ThrowIfUnnestInLambda(const ColumnBinding &column_binding) override;
	BindResult BindUnnest(FunctionExpression &function, idx_t depth, bool root_expression) override;

	unique_ptr<ParsedExpression> GetSQLValueFunction(const string &column_name) override;

protected:
	idx_t unnest_level = 0;
};

} // namespace duckdb
