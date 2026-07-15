//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/flatten_dependent_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalAggregate;
class LogicalComparisonJoin;
class LogicalCTERef;
class LogicalDependentJoin;
class LogicalExpressionGet;
class LogicalJoin;

//! The FlattenDependentJoins class is responsible for pushing the dependent join down into the plan to create a
//! flattened subquery
class FlattenDependentJoins {
public:
	static unique_ptr<LogicalOperator> DecorrelateIndependent(Binder &binder, unique_ptr<LogicalOperator> plan);

private:
	struct UnnestingState {
		UnnestingState() = default;
		explicit UnnestingState(vector<ColumnBinding> bindings_p) : bindings(std::move(bindings_p)) {
		}

		vector<ColumnBinding> bindings;
		vector<ReplacementBinding> binding_replacements;
	};

	FlattenDependentJoins(Binder &binder, const CorrelatedColumns &correlated, bool perform_delim = true,
	                      bool any_join = false, optional_ptr<FlattenDependentJoins> parent = nullptr);
	vector<ColumnBinding> CreateContiguousState(ColumnBinding base_binding) const;

	UnnestingState DecorrelateSubtree(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                  vector<ColumnBinding> state);
	static void CreateDelimJoinConditions(LogicalComparisonJoin &delim_join,
	                                      const CorrelatedColumns &correlated_columns,
	                                      const vector<ColumnBinding> &state, bool perform_delim);
	column_binding_map_t<ColumnBinding> GetCurrentBindings(const vector<ColumnBinding> &state) const;
	//! Checks whether a subtree contains any correlated expressions that reference this flattener's correlated columns.
	bool DependsOnCorrelated(LogicalOperator &op) const;
	idx_t GetDelimKeyIndex(idx_t index) const;

	UnnestingState PushDownCorrelatedNode(unique_ptr<LogicalOperator> &plan, bool propagate_null_values = true);
	UnnestingState PushDownCorrelatedNode(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                      vector<ColumnBinding> state);
	optional_ptr<const ColumnBinding> GetCorrelatedBase(const ColumnBinding &binding) const;
	optional_idx GetCorrelatedIndex(const ColumnBinding &binding) const;
	void MergeCorrelatedAliases(const FlattenDependentJoins &source);
	Binder &binder;
	column_binding_map_t<ColumnBinding> correlated_aliases;
	column_binding_map_t<idx_t> replacement_map;
	const CorrelatedColumns &correlated_columns;
	vector<LogicalType> delim_types;

	bool perform_delim;
	bool any_join;
	optional_ptr<FlattenDependentJoins> parent;
	mutable reference_map_t<LogicalOperator, bool> dependency_cache;
	static ColumnBinding ResolveBinding(ColumnBinding binding, const vector<ReplacementBinding> &replacements);
	static void AddBindingReplacement(UnnestingState &state, ColumnBinding old_binding, ColumnBinding new_binding);
	static void MergeBindingReplacements(UnnestingState &target, const UnnestingState &source);
	static void RewriteOperatorBindings(LogicalOperator &op, const UnnestingState &state);
	static vector<ColumnBinding> GetRightPayloadBindings(LogicalDependentJoin &op);
	void AppendCorrelatedColumns(vector<unique_ptr<Expression>> &expressions, const vector<ColumnBinding> &state,
	                             bool include_names) const;
	void AddDelimColumnsToGroup(LogicalAggregate &aggr, const vector<ColumnBinding> &state) const;
	void AddCorrelatedFirstAggregates(LogicalAggregate &aggr, const vector<ColumnBinding> &state) const;
	void AddAnyJoinConditions(LogicalDependentJoin &op, const vector<ColumnBinding> &plan_columns) const;
	void AddCTERefJoinConditions(LogicalComparisonJoin &join, const LogicalCTERef &cteref,
	                             const vector<ColumnBinding> &state) const;
	void AddCorrelatedJoinConditions(LogicalJoin &join, const vector<ColumnBinding> &left_state,
	                                 const vector<ColumnBinding> &right_state) const;
	vector<ColumnBinding> CreateDelimCrossProduct(unique_ptr<LogicalOperator> &plan,
	                                              unique_ptr<LogicalOperator> delim_scan,
	                                              vector<ColumnBinding> state) const;
	void PatchAccessingOperators(LogicalOperator &subtree_root, TableIndex table_index,
	                             const CorrelatedColumns &correlated_columns);
	UnnestingState PrepareDependentJoinLeft(LogicalDependentJoin &op, bool propagate_null_values,
	                                        vector<ColumnBinding> state);
	UnnestingState PushDownChild(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                             vector<ColumnBinding> state, bool rewrite_parent = true, idx_t child_idx = 0);
	UnnestingState FinalizeDependentJoin(unique_ptr<LogicalOperator> &plan, UnnestingState outer_state,
	                                     const UnnestingState &right_state,
	                                     vector<ColumnBinding> right_payload_bindings);
	vector<ColumnBinding> AttachDelimToIndependentJoinLeft(unique_ptr<LogicalOperator> &left, LogicalJoin &join);
	UnnestingState PushDownSingleCorrelatedChild(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                             vector<ColumnBinding> state, bool correlated_left);
	UnnestingState PushDownProjection(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                  vector<ColumnBinding> state);
	UnnestingState PushDownAggregate(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                 vector<ColumnBinding> state);
	UnnestingState PushDownCrossProduct(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                    vector<ColumnBinding> state);
	UnnestingState PushDownFullOuterJoin(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                     vector<ColumnBinding> state);
	UnnestingState PushDownJoin(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                            vector<ColumnBinding> state);
	UnnestingState PushDownLimit(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                             vector<ColumnBinding> state);
	UnnestingState PushDownWindow(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                              vector<ColumnBinding> state);
	UnnestingState PushDownSetOperation(unique_ptr<LogicalOperator> &plan, vector<ColumnBinding> state);
	UnnestingState PushDownDistinct(unique_ptr<LogicalOperator> &plan, vector<ColumnBinding> state);
	UnnestingState PushDownExpressionGet(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                     vector<ColumnBinding> state);
	UnnestingState PushDownDML(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                           vector<ColumnBinding> state);
	UnnestingState PushDownGet(unique_ptr<LogicalOperator> &plan, vector<ColumnBinding> state);
	UnnestingState PushDownCTE(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                           vector<ColumnBinding> state);
	UnnestingState PushDownCTERef(unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
