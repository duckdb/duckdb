//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/column_alias_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

class ColumnRefExpression;
struct SelectBindState;

//! A helper binder for WhereBinder and HavingBinder which support alias as a columnref.
class ColumnAliasBinder {
public:
	explicit ColumnAliasBinder(SelectBindState &bind_state);

	bool BindAlias(ExpressionBinder &enclosing_binder, unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	               bool root_expression, BindResult &result);
	// Check if the column reference is an SELECT item alias.
	bool QualifyColumnAlias(const ColumnRefExpression &colref);

private:
	SelectBindState &bind_state;
	unordered_set<idx_t> visited_select_indexes;
};

} // namespace duckdb
