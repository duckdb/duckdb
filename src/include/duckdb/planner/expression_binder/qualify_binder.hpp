//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/qualify_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/expression_binder/column_alias_binder.hpp"

namespace duckdb {

//! The QUALIFY binder is responsible for binding an expression within the QUALIFY clause of a SQL statement
class QualifyBinder : public SelectBinder {
public:
	QualifyBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
	              unordered_map<string, idx_t> &alias_map);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

private:
	BindResult BindColumnRef(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression);

	ColumnAliasBinder column_alias_binder;
};

} // namespace duckdb
