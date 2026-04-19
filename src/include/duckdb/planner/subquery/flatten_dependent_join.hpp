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
	FlattenDependentJoins(Binder &binder, const CorrelatedColumns &correlated, bool perform_delim = true,
	                      bool any_join = false, optional_ptr<FlattenDependentJoins> parent = nullptr);
	vector<ColumnBinding> InitialState() const {
		return {};
	}
	vector<ColumnBinding> CreateContiguousState(ColumnBinding base_binding) const;

	vector<ColumnBinding> Decorrelate(unique_ptr<LogicalOperator> &plan) {
		return Decorrelate(plan, true, InitialState());
	}
	vector<ColumnBinding> Decorrelate(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                  vector<ColumnBinding> state);
	static void CreateDelimJoinConditions(LogicalComparisonJoin &delim_join,
	                                      const CorrelatedColumns &correlated_columns,
	                                      const vector<ColumnBinding> &state, bool perform_delim);
	//! Checks whether a subtree contains any correlated expressions that reference this flattener's correlated columns.
	bool DependsOnCorrelated(LogicalOperator &op) const;
	idx_t GetDelimKeyIndex(idx_t index) const;

	//! Push the dependent join down a LogicalOperator
	vector<ColumnBinding> PushDownDependentJoin(unique_ptr<LogicalOperator> &plan, bool propagate_null_values = true) {
		return PushDownDependentJoin(plan, propagate_null_values, InitialState());
	}
	vector<ColumnBinding> PushDownDependentJoin(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                            vector<ColumnBinding> state);
	vector<ColumnBinding> DecorrelateDependentJoin(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                               vector<ColumnBinding> state);
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
	void AppendDelimColumns(vector<unique_ptr<Expression>> &expressions, const vector<ColumnBinding> &state,
	                        bool include_names) const;
	void AppendCorrelatedColumns(vector<unique_ptr<Expression>> &expressions, const vector<ColumnBinding> &state,
	                             bool include_names) const;
	void AddDelimColumnsToGroup(LogicalAggregate &aggr, const vector<ColumnBinding> &state) const;
	void AddCorrelatedFirstAggregates(LogicalAggregate &aggr, const vector<ColumnBinding> &state) const;
	void AddAnyJoinConditions(LogicalDependentJoin &op, const vector<ColumnBinding> &plan_columns) const;
	static void PopulateDuplicateEliminatedColumns(LogicalDependentJoin &op);
	void AddComparisonJoinConditions(LogicalComparisonJoin &join, const vector<ColumnBinding> &left_state,
	                                 const vector<ColumnBinding> &right_state) const;
	void AddCTERefJoinConditions(LogicalComparisonJoin &join, const LogicalCTERef &cteref,
	                             const vector<ColumnBinding> &state) const;
	void AddCorrelatedJoinConditions(LogicalJoin &join, const vector<ColumnBinding> &left_state,
	                                 const vector<ColumnBinding> &right_state) const;
	vector<ColumnBinding> CreateDelimCrossProduct(unique_ptr<LogicalOperator> &plan,
	                                              unique_ptr<LogicalOperator> delim_scan,
	                                              vector<ColumnBinding> state) const;
	void PatchAccessingOperators(LogicalOperator &subtree_root, TableIndex table_index,
	                             const CorrelatedColumns &correlated_columns);
	vector<ColumnBinding> PrepareDependentJoinLeft(LogicalDependentJoin &op, bool propagate_null_values,
	                                               vector<ColumnBinding> state);
	vector<ColumnBinding> FinalizeDependentJoin(unique_ptr<LogicalOperator> &plan, vector<ColumnBinding> outer_state,
	                                            const vector<ColumnBinding> &right_state);
	vector<ColumnBinding> PushDownSingleCorrelatedChild(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                                    vector<ColumnBinding> state, bool correlated_left);
	vector<ColumnBinding> PushDownFilter(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                     vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownProjection(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                         vector<ColumnBinding> state);
	vector<ColumnBinding> FinalizeProjection(unique_ptr<LogicalOperator> &plan, const vector<ColumnBinding> &state,
	                                         const vector<ColumnBinding> &old_child_bindings);
	vector<ColumnBinding> PushDownAggregate(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                        vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownCrossProduct(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                           vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownJoin(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                   vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownLimit(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                    vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownWindow(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                     vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownSetOperation(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                           vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownDistinct(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                       vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownExpressionGet(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                            vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownOrderBy(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                      vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownGet(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                  vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownCTE(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                  vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownCTERef(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                     vector<ColumnBinding> state);
	vector<ColumnBinding> PushDownDependentJoinInternal(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
	                                                    vector<ColumnBinding> state);
};

} // namespace duckdb
