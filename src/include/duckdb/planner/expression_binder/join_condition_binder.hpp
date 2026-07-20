//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/join_condition_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder/where_binder.hpp"

namespace duckdb {

//! Binds a join condition against its complete lexical scope while representing one input as a lateral correlation.
class JoinConditionBinder : public WhereBinder {
public:
	JoinConditionBinder(Binder &binder, ClientContext &context, const unordered_set<TableIndex> &lateral_bindings);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression = false) override;

private:
	const unordered_set<TableIndex> &lateral_bindings;
};

} // namespace duckdb
