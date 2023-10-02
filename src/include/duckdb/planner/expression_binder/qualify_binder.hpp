//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/qualify_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder/base_select_binder.hpp"
#include "duckdb/planner/expression_binder/column_alias_binder.hpp"

namespace duckdb {

//! The QUALIFY binder is responsible for binding an expression within the QUALIFY clause of a SQL statement
class QualifyBinder : public BaseSelectBinder {
public:
	QualifyBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
	              case_insensitive_map_t<idx_t> &alias_map);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

private:
	BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression);

	ColumnAliasBinder column_alias_binder;
};

} // namespace duckdb
