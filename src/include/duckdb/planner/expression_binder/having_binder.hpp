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
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {
class Binder;
class BoundSelectNode;
class ClientContext;
class ColumnRefExpression;
class ErrorData;
class LambdaRefExpression;
class WindowExpression;
enum class AggregateHandling : uint8_t;

//! The HAVING binder is responsible for binding an expression within the HAVING clause of a SQL statement.
class HavingBinder : public BaseSelectBinder {
public:
	HavingBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
	             AggregateHandling aggregate_handling);

protected:
	BindResult BindLambdaReference(LambdaRefExpression &expr, idx_t depth);
	BindResult BindWindow(WindowExpression &expr, idx_t depth) override;
	BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override;

	unique_ptr<ParsedExpression> QualifyColumnName(ColumnRefExpression &col_ref, ErrorData &error) override;

	bool DoesColumnAliasExist(const ColumnRefExpression &colref) override;

private:
	ColumnAliasBinder column_alias_binder;
	AggregateHandling aggregate_handling;
};

} // namespace duckdb
