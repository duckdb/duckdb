//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/select_bind_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/parser/group_by_node.hpp"

namespace duckdb {

//! Bind state during a SelectNode
struct SelectBindState {
	// Mapping of (alias -> index) and a mapping of (Expression -> index) for the SELECT list
	case_insensitive_map_t<idx_t> alias_map;
	parsed_expression_map_t<idx_t> projection_map;
	//! The original unparsed expressions. This is exported after binding, because the binding might change the
	//! expressions (e.g. when a * clause is present)
	vector<unique_ptr<ParsedExpression>> original_expressions;

public:
	unique_ptr<ParsedExpression> BindAlias(idx_t index);

	void SetExpressionIsVolatile(idx_t index);
	void SetExpressionHasSubquery(idx_t index);

	bool AliasHasSubquery(idx_t index) const;

	void AddExpandedColumn(idx_t expand_count);
	void AddRegularColumn();
	idx_t GetFinalIndex(idx_t index) const;

private:
	//! The set of referenced aliases
	unordered_set<idx_t> referenced_aliases;
	//! The set of expressions that is volatile
	unordered_set<idx_t> volatile_expressions;
	//! The set of expressions that contains a subquery
	unordered_set<idx_t> subquery_expressions;
	//! Column indices after expansion of Expanded expressions (e.g. UNNEST(STRUCT) clauses)
	vector<idx_t> expanded_column_indices;
};

} // namespace duckdb
