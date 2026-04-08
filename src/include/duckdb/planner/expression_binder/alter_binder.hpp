//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/alter_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class TableCatalogEntry;
class Binder;
class ClientContext;
class ColumnRefExpression;
class LambdaRefExpression;
class ParsedExpression;
struct LogicalIndex;

//! The AlterBinder binds expressions in ALTER statements.
class AlterBinder : public ExpressionBinder {
public:
	AlterBinder(Binder &binder, ClientContext &context, TableCatalogEntry &table, vector<LogicalIndex> &bound_columns,
	            LogicalType target_type);

protected:
	BindResult BindLambdaReference(LambdaRefExpression &expr, idx_t depth);
	BindResult BindColumnReference(ColumnRefExpression &expr, idx_t depth);
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;

private:
	TableCatalogEntry &table;
	vector<LogicalIndex> &bound_columns;
};

} // namespace duckdb
