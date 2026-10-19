//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/having_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder/base_select_binder.hpp"
#include "duckdb/planner/expression_binder/column_alias_binder.hpp"
#include "duckdb/common/enums/aggregate_handling.hpp"

namespace duckdb {

//! The HAVING binder is responsible for binding an expression within the HAVING clause of a SQL statement.
class HavingBinder : public BaseSelectBinder {
public:
	HavingBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, AggregateHandling aggregate_handling);

public:
	unique_ptr<ColumnQualifier> CreateColumnQualifier() override;

protected:
	BindResult BindLambdaReference(const LambdaRefExpression &expr, idx_t depth);
	BindResult BindWindowExpression(const WindowExpression &expr, idx_t depth) override;
	BindResult BindColumnRef(const ParsedExpression &expr, idx_t depth, bool root_expression) override;

public:
	ColumnAliasBinder column_alias_binder;

public:
	bool ClaimsAlias(const ColumnRefExpression &colref) override;

private:
	AggregateHandling aggregate_handling;
};

} // namespace duckdb
