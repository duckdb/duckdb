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
struct SelectBindState;

//! The QUALIFY binder is responsible for binding an expression within the QUALIFY clause of a SQL statement
class QualifyBinder : public BaseSelectBinder {
public:
	QualifyBinder(Binder &binder, ClientContext &context, BoundSelectNode &node);

protected:
	BindResult BindColumnRef(const ParsedExpression &expr, idx_t depth, bool root_expression) override;

private:
	ColumnAliasBinder column_alias_binder;

public:
	bool ClaimsAlias(const ColumnRefExpression &colref) override;
};

} // namespace duckdb
