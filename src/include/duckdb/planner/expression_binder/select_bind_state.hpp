//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/select_bind_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/planner/expression_binder/expression_match.hpp"

namespace duckdb {

//! Bind state during a SelectNode
struct SelectBindState {
	explicit SelectBindState(ExpressionMatchPolicy projection_policy = ExpressionMatchPolicy::REPLACE);

	// Mapping of (alias -> index) and a mapping of (Expression -> index) for the SELECT list
	identifier_map_t<idx_t> alias_map;
	ExpressionMatchIndex projection_index;
	identifier_map_t<ProjectionIndex> group_alias_map;
	//! Whether any group is an expression other than a plain column reference
	bool has_compound_groups = false;
	unordered_map<ProjectionIndex, ProjectionIndex> collated_groups;
	unordered_set<idx_t> used_group_aliases;

public:
	unique_ptr<ParsedExpression> BindAlias(idx_t index);
	void AddProjection(Binder &binder, ParsedExpression &expr);
	void CompleteProjection(idx_t index);
	idx_t ProjectionCount() const;
	const ParsedExpression &ProjectionExpression(idx_t index) const;
	shared_ptr<const ExpressionMatchDescriptor> ProjectionDescriptor(idx_t index) const;

	void AddGroup(shared_ptr<const ExpressionMatchDescriptor> expression, ProjectionIndex index);
	void ApplyGroupBinding(const ParsedExpression &expr, ProjectionIndex index) const;
	ProjectionIndex FindGroup(const ParsedExpression &expr) const;

	void SetExpressionIsVolatile(idx_t index);
	void SetExpressionHasSubquery(idx_t index);

	bool AliasHasSubquery(idx_t index) const;

	void AddExpandedColumn(idx_t expand_count);
	void AddRegularColumn();
	ProjectionIndex GetFinalIndex(idx_t index) const;

private:
	struct Projection {
		unique_ptr<ParsedExpression> snapshot;
		shared_ptr<const ExpressionMatchDescriptor> descriptor;
	};
	vector<Projection> projections;
	ExpressionMatchIndex group_index;
	vector<shared_ptr<const ExpressionMatchDescriptor>> groups;
	//! The set of referenced aliases
	unordered_set<idx_t> referenced_aliases;
	//! The set of expressions that is volatile
	unordered_set<idx_t> volatile_expressions;
	//! The set of expressions that contains a subquery
	unordered_set<idx_t> subquery_expressions;
	//! Column indices after expansion of Expanded expressions (e.g. UNNEST(STRUCT) clauses)
	vector<ProjectionIndex> expanded_column_indices;
};

} // namespace duckdb
