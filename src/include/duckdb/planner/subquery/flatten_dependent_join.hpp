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

//! The FlattenDependentJoins class is responsible for pushing the dependent join down into the plan to create a
//! flattened subquery
struct FlattenDependentJoins {
	struct PushDownState {
		PushDownState() {
		}
		PushDownState(vector<ColumnBinding> correlated_bindings_p, vector<idx_t> correlated_offsets_p)
		    : correlated_bindings(std::move(correlated_bindings_p)),
		      correlated_offsets(std::move(correlated_offsets_p)) {
			D_ASSERT(correlated_bindings.size() == correlated_offsets.size());
		}

		static PushDownState CreateContiguous(ColumnBinding base_binding, idx_t correlated_offset, idx_t count) {
			vector<ColumnBinding> correlated_bindings;
			vector<idx_t> correlated_offsets;
			for (idx_t i = 0; i < count; i++) {
				correlated_bindings.emplace_back(base_binding.table_index,
				                                 ProjectionIndex(base_binding.column_index + i));
				correlated_offsets.push_back(correlated_offset + i);
			}
			return PushDownState(std::move(correlated_bindings), std::move(correlated_offsets));
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

		vector<ColumnBinding> correlated_bindings;
		vector<idx_t> correlated_offsets;
	};

	struct PushDownResult {
		PushDownResult(unique_ptr<LogicalOperator> plan_p, PushDownState state_p, bool propagate_null_values_p = true)
		    : plan(std::move(plan_p)), state(std::move(state_p)), propagate_null_values(propagate_null_values_p) {
		}

		unique_ptr<LogicalOperator> plan;
		PushDownState state;
		bool propagate_null_values;
	};

	FlattenDependentJoins(Binder &binder, const CorrelatedColumns &correlated, bool perform_delim = true,
	                      bool any_join = false, optional_ptr<FlattenDependentJoins> parent = nullptr);

	static unique_ptr<LogicalOperator> DecorrelateIndependent(Binder &binder, unique_ptr<LogicalOperator> plan);

	PushDownResult Decorrelate(unique_ptr<LogicalOperator> plan, bool parent_propagate_null_values = true,
	                           idx_t lateral_depth = 0, PushDownState state = PushDownState());

private:
	//! Detects which Logical Operators have correlated expressions that they are dependent upon, filling the
	//! has_correlated_expressions map.
	bool DetectCorrelatedExpressions(LogicalOperator &op, bool lateral = false, idx_t lateral_depth = 0,
	                                 bool parent_is_dependent_join = false);

	//! Mark entire subtree of Logical Operators as correlated by adding them to the has_correlated_expressions map.
	bool MarkSubtreeCorrelated(LogicalOperator &op, TableIndex cte_index);

	//! Push the dependent join down a LogicalOperator
	PushDownResult PushDownDependentJoin(unique_ptr<LogicalOperator> plan, bool propagates_null_values = true,
	                                     idx_t lateral_depth = 0, PushDownState state = PushDownState());

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
	PushDownResult PushDownFilter(unique_ptr<LogicalOperator> plan, bool parent_propagate_null_values,
	                              idx_t lateral_depth, PushDownState state);
	PushDownResult PushDownUnnest(unique_ptr<LogicalOperator> plan, bool parent_propagate_null_values,
	                              idx_t lateral_depth, PushDownState state);
	PushDownResult PushDownProjection(unique_ptr<LogicalOperator> plan, bool parent_propagate_null_values,
	                                  idx_t lateral_depth, PushDownState state, bool exit_projection,
	                                  unique_ptr<LogicalOperator> delim_scan);
	PushDownResult PushDownAggregate(unique_ptr<LogicalOperator> plan, bool parent_propagate_null_values,
	                                 idx_t lateral_depth, PushDownState state);
	PushDownResult PushDownDependentJoinInternal(unique_ptr<LogicalOperator> plan, bool parent_propagate_null_values,
	                                             idx_t lateral_depth, PushDownState state);
};

} // namespace duckdb
