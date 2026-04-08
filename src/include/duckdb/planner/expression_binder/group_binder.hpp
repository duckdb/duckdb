//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/group_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_set>

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
class ConstantExpression;
class ColumnRefExpression;
struct SelectBindState;
class Binder;
class ClientContext;
class SelectNode;

//! The GROUP binder is responsible for binding expressions in the GROUP BY clause
class GroupBinder : public ExpressionBinder {
public:
	GroupBinder(Binder &binder, ClientContext &context, SelectNode &node, TableIndex group_index,
	            SelectBindState &bind_state, case_insensitive_map_t<ProjectionIndex> &group_alias_map);

	//! The unbound root expression
	unique_ptr<ParsedExpression> unbound_expression;
	//! The group index currently being bound
	ProjectionIndex bind_index;

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override;

	string UnsupportedAggregateMessage() override;

	BindResult BindSelectRef(idx_t entry);
	BindResult BindColumnRef(ColumnRefExpression &expr, unique_ptr<ParsedExpression> &expr_ptr);
	BindResult BindConstant(ConstantExpression &expr);

	bool TryResolveAliasReference(ColumnRefExpression &colref, idx_t depth, bool root_expression, BindResult &result,
	                              unique_ptr<ParsedExpression> &expr_ptr) override;

	SelectNode &node;
	SelectBindState &bind_state;
	case_insensitive_map_t<ProjectionIndex> &group_alias_map;
	unordered_set<idx_t> used_aliases;

	TableIndex group_index;
};

} // namespace duckdb
