//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/flatten_dependent_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalAggregate;
class LogicalExpressionGet;

//! The FlattenDependentJoins class is responsible for pushing the dependent join down into the plan to create a
//! flattened subquery
struct FlattenDependentJoins {
	struct PushDownState {
		PushDownState() {
		}
		PushDownState(vector<ColumnBinding> correlated_bindings_p, vector<idx_t> correlated_offsets_p,
		              bool propagate_null_values_p = true, idx_t lateral_depth_p = 0)
		    : correlated_bindings(std::move(correlated_bindings_p)),
		      correlated_offsets(std::move(correlated_offsets_p)), propagate_null_values(propagate_null_values_p),
		      lateral_depth(lateral_depth_p) {
			D_ASSERT(correlated_bindings.size() == correlated_offsets.size());
		}

		static PushDownState CreateContiguous(const PushDownState &state, ColumnBinding base_binding,
		                                      idx_t correlated_offset, idx_t count) {
			vector<ColumnBinding> correlated_bindings;
			vector<idx_t> correlated_offsets;
			for (idx_t i = 0; i < count; i++) {
				correlated_bindings.emplace_back(base_binding.table_index,
				                                 ProjectionIndex(base_binding.column_index + i));
				correlated_offsets.push_back(correlated_offset + i);
			}
			return PushDownState(std::move(correlated_bindings), std::move(correlated_offsets),
			                     state.propagate_null_values, state.lateral_depth);
		}

		const ColumnBinding &GetBinding(idx_t index) const {
			D_ASSERT(index < correlated_bindings.size());
			return correlated_bindings[index];
		}

		idx_t GetOffset(idx_t index) const {
			D_ASSERT(index < correlated_offsets.size());
			return correlated_offsets[index];
		}

		void ShiftOffsets(idx_t offset) {
			for (auto &entry : correlated_offsets) {
				entry += offset;
			}
		}

		void UpdateTraversalState(bool propagate_null_values_p, idx_t lateral_depth_p) {
			propagate_null_values = propagate_null_values_p;
			lateral_depth = lateral_depth_p;
		}

		void UpdateTraversalState(const PushDownState &state) {
			UpdateTraversalState(state.propagate_null_values, state.lateral_depth);
		}

		PushDownState WithTraversalState(bool propagate_null_values_p, idx_t lateral_depth_p) const {
			auto result = *this;
			result.UpdateTraversalState(propagate_null_values_p, lateral_depth_p);
			return result;
		}

		PushDownState WithFreshTraversal() const {
			return WithTraversalState(true, 0);
		}

		vector<ColumnBinding> correlated_bindings;
		vector<idx_t> correlated_offsets;
		bool propagate_null_values = true;
		idx_t lateral_depth = 0;
	};

	struct PushDownResult {
		PushDownResult(unique_ptr<LogicalOperator> plan_p, PushDownState state_p)
		    : plan(std::move(plan_p)), state(std::move(state_p)) {
		}

		unique_ptr<LogicalOperator> plan;
		PushDownState state;
	};

	FlattenDependentJoins(Binder &binder, const CorrelatedColumns &correlated, bool perform_delim = true,
	                      bool any_join = false, optional_ptr<FlattenDependentJoins> parent = nullptr);

	static unique_ptr<LogicalOperator> DecorrelateIndependent(Binder &binder, unique_ptr<LogicalOperator> plan);

	PushDownResult Decorrelate(unique_ptr<LogicalOperator> plan, PushDownState state = PushDownState());

private:
	//! Detects which Logical Operators have correlated expressions that they are dependent upon, filling the
	//! has_correlated_expressions map.
	bool DetectCorrelatedExpressions(LogicalOperator &op, bool lateral = false, idx_t lateral_depth = 0,
	                                 bool parent_is_dependent_join = false);

	//! Mark entire subtree of Logical Operators as correlated by adding them to the has_correlated_expressions map.
	bool MarkSubtreeCorrelated(LogicalOperator &op, TableIndex cte_index);

	//! Push the dependent join down a LogicalOperator
	PushDownResult PushDownDependentJoin(unique_ptr<LogicalOperator> plan, PushDownState state = PushDownState());

public:
	Binder &binder;
	reference_map_t<LogicalOperator, bool> has_correlated_expressions;
	column_binding_map_t<idx_t> correlated_map;
	column_binding_map_t<idx_t> replacement_map;
	const CorrelatedColumns &correlated_columns;
	vector<LogicalType> delim_types;

	bool perform_delim;
	bool any_join;
	optional_ptr<FlattenDependentJoins> parent;

private:
	PushDownState CreateCorrelatedState(const PushDownState &state, TableIndex table_index, idx_t binding_offset,
	                                    idx_t correlated_offset) const;
	PushDownState CreateCorrelatedState(const PushDownState &state, TableIndex table_index,
	                                    idx_t correlated_offset) const;
	PushDownState CreateLeadingCorrelatedState(const PushDownState &state, const vector<ColumnBinding> &bindings) const;
	void AppendCorrelatedColumns(vector<unique_ptr<Expression>> &expressions, const PushDownState &state, idx_t count,
	                             bool include_names) const;
	void AppendCorrelatedColumnsToExpressionGet(LogicalExpressionGet &expr_get, const PushDownState &state) const;
	void AddCorrelatedGroupColumns(LogicalAggregate &aggr, const PushDownState &state, idx_t group_count) const;
	void AddCorrelatedFirstAggregates(LogicalAggregate &aggr, const PushDownState &state) const;
	PushDownState PushDownChild(unique_ptr<LogicalOperator> &child, PushDownState state);
	PushDownState PushDownChildFresh(unique_ptr<LogicalOperator> &child, const PushDownState &state);
	PushDownResult PushDownFilter(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownUnnest(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownProjection(unique_ptr<LogicalOperator> plan, PushDownState state, bool exit_projection,
	                                  unique_ptr<LogicalOperator> delim_scan);
	PushDownResult PushDownAggregate(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownCrossProduct(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownJoin(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownLimit(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownWindow(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownSetOperation(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownDistinct(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownExpressionGet(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownOrderBy(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownGet(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownCTE(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownCTERef(unique_ptr<LogicalOperator> plan, PushDownState state);
	PushDownResult PushDownDependentJoinInternal(unique_ptr<LogicalOperator> plan, PushDownState state);
};

} // namespace duckdb
