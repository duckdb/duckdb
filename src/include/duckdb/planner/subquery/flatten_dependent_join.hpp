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
	using CorrelatedState = vector<ColumnBinding>;

private:
	FlattenDependentJoins(Binder &binder, const CorrelatedColumns &correlated, bool perform_delim = true,
	                      bool any_join = false, optional_ptr<FlattenDependentJoins> parent = nullptr);
	CorrelatedState InitialState() const {
		return {};
	}
	CorrelatedState CreateContiguousState(ColumnBinding base_binding) const;
	CorrelatedState CreateLeadingState(const vector<ColumnBinding> &bindings) const;
	void RewriteCorrelated(LogicalOperator &op, const CorrelatedState &state);
	void AssertUsableState(const CorrelatedState &state) const;

	CorrelatedState Decorrelate(unique_ptr<LogicalOperator> &plan) {
		return Decorrelate(plan, true, InitialState());
	}
	CorrelatedState Decorrelate(unique_ptr<LogicalOperator> &plan, bool propagate_null_values, CorrelatedState state);
	static void CreateDelimJoinConditions(LogicalComparisonJoin &delim_join,
	                                      const CorrelatedColumns &correlated_columns, const CorrelatedState &state,
	                                      bool perform_delim);
	//! Checks whether a subtree contains any correlated expressions that reference this flattener's correlated columns.
	bool DependsOnCorrelated(LogicalOperator &op) const;
	idx_t GetDelimKeyCount(const CorrelatedState &state, bool perform_delim) const;
	idx_t GetDelimKeyIndex(const CorrelatedState &state, idx_t index, bool perform_delim) const;
	const CorrelatedColumnInfo &GetDelimKey(const CorrelatedState &state, idx_t index, bool perform_delim) const;
	const ColumnBinding &GetDelimBinding(const CorrelatedState &state, idx_t index, bool perform_delim) const;

	//! Push the dependent join down a LogicalOperator
	CorrelatedState PushDownDependentJoin(unique_ptr<LogicalOperator> &plan, bool propagate_null_values = true) {
		return PushDownDependentJoin(plan, propagate_null_values, InitialState());
	}
	CorrelatedState PushDownDependentJoin(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                      CorrelatedState state);
	CorrelatedState DecorrelateDependentJoin(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                         CorrelatedState state);
	optional_ptr<const ColumnBinding> GetCorrelatedBase(const ColumnBinding &binding) const;
	optional_idx GetCorrelatedIndexByBase(const ColumnBinding &base_binding) const;
	optional_idx GetCorrelatedIndex(const ColumnBinding &binding) const;
	void MergeCorrelatedAliases(const FlattenDependentJoins &source);
	Binder &binder;
	vector<ColumnBinding> correlated_base_bindings;
	column_binding_map_t<ColumnBinding> correlated_aliases;
	column_binding_map_t<idx_t> replacement_map;
	const CorrelatedColumns &correlated_columns;
	vector<LogicalType> delim_types;

	bool perform_delim;
	bool any_join;
	optional_ptr<FlattenDependentJoins> parent;
	mutable reference_map_t<LogicalOperator, bool> dependency_cache;
	void AppendDelimColumns(vector<unique_ptr<Expression>> &expressions, const CorrelatedState &state,
	                        bool include_names) const;
	void AppendCorrelatedColumns(vector<unique_ptr<Expression>> &expressions, const CorrelatedState &state,
	                             bool include_names) const;
	void AddDelimColumnsToGroup(LogicalAggregate &aggr, const CorrelatedState &state) const;
	void AddCorrelatedFirstAggregates(LogicalAggregate &aggr, const CorrelatedState &state) const;
	void AddAnyJoinConditions(LogicalDependentJoin &op, const vector<ColumnBinding> &plan_columns) const;
	static vector<ColumnBinding> GetDependentJoinPlanColumns(LogicalOperator &op);
	static void PopulateDuplicateEliminatedColumns(LogicalDependentJoin &op);
	void AddComparisonJoinConditions(LogicalComparisonJoin &join, const CorrelatedState &left_state,
	                                 const CorrelatedState &right_state) const;
	void AddCTERefJoinConditions(LogicalComparisonJoin &join, const LogicalCTERef &cteref,
	                             const CorrelatedState &state) const;
	void AddCorrelatedJoinConditions(LogicalJoin &join, const CorrelatedState &left_state,
	                                 const CorrelatedState &right_state) const;
	CorrelatedState CreateDelimCrossProduct(unique_ptr<LogicalOperator> &plan, unique_ptr<LogicalOperator> delim_scan,
	                                        CorrelatedState state) const;
	void PatchAccessingOperators(LogicalOperator &subtree_root, TableIndex table_index,
	                             const CorrelatedColumns &correlated_columns);
	CorrelatedState PrepareDependentJoinLeft(LogicalDependentJoin &op, bool propagate_null_values,
	                                         CorrelatedState state);
	CorrelatedState FinalizeDependentJoin(unique_ptr<LogicalOperator> &plan, CorrelatedState outer_state,
	                                      const CorrelatedState &right_state);
	CorrelatedState PushDownSingleCorrelatedChild(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                              CorrelatedState state, bool correlated_left);
	CorrelatedState PushDownFilter(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                               CorrelatedState state);
	CorrelatedState PushDownProjection(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                   CorrelatedState state);
	CorrelatedState FinalizeProjection(unique_ptr<LogicalOperator> &plan, CorrelatedState state,
	                                   const vector<ColumnBinding> &old_child_bindings);
	CorrelatedState PushDownAggregate(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                  CorrelatedState state);
	CorrelatedState PushDownCrossProduct(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                     CorrelatedState state);
	CorrelatedState PushDownJoin(unique_ptr<LogicalOperator> &plan, bool propagate_null_values, CorrelatedState state);
	CorrelatedState PushDownLimit(unique_ptr<LogicalOperator> &plan, bool propagate_null_values, CorrelatedState state);
	CorrelatedState PushDownWindow(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                               CorrelatedState state);
	CorrelatedState PushDownSetOperation(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                     CorrelatedState state);
	CorrelatedState PushDownDistinct(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                 CorrelatedState state);
	CorrelatedState PushDownExpressionGet(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                      CorrelatedState state);
	CorrelatedState PushDownOrderBy(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                CorrelatedState state);
	CorrelatedState PushDownGet(unique_ptr<LogicalOperator> &plan, bool propagate_null_values, CorrelatedState state);
	CorrelatedState PushDownCTE(unique_ptr<LogicalOperator> &plan, bool propagate_null_values, CorrelatedState state);
	CorrelatedState PushDownCTERef(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                               CorrelatedState state);
	CorrelatedState PushDownDependentJoinInternal(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                              CorrelatedState state);
};

} // namespace duckdb
