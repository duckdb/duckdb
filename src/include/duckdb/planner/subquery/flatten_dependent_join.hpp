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
class LogicalComparisonJoin;
class LogicalDependentJoin;
class LogicalExpressionGet;
class LogicalJoin;

//! The FlattenDependentJoins class is responsible for pushing the dependent join down into the plan to create a
//! flattened subquery
struct FlattenDependentJoins {
	struct PushDownContext {
		PushDownContext() {
		}
		PushDownContext(bool propagate_null_values_p, idx_t lateral_depth_p)
		    : propagate_null_values(propagate_null_values_p), lateral_depth(lateral_depth_p) {
		}

		PushDownContext WithPropagateNullValues(bool propagate_null_values_p) const {
			return PushDownContext(propagate_null_values_p, lateral_depth);
		}

		PushDownContext WithLateralDepth(idx_t lateral_depth_p) const {
			return PushDownContext(propagate_null_values, lateral_depth_p);
		}

		PushDownContext WithFreshTraversal() const {
			return PushDownContext();
		}

		bool propagate_null_values = true;
		idx_t lateral_depth = 0;
	};

	struct CorrelatedLayout {
		CorrelatedLayout() {
		}
		CorrelatedLayout(vector<ColumnBinding> correlated_bindings_p, vector<idx_t> correlated_offsets_p)
		    : correlated_bindings(std::move(correlated_bindings_p)),
		      correlated_offsets(std::move(correlated_offsets_p)) {
			D_ASSERT(correlated_bindings.size() == correlated_offsets.size());
		}

		static CorrelatedLayout CreateContiguous(ColumnBinding base_binding, idx_t correlated_offset, idx_t count) {
			vector<ColumnBinding> correlated_bindings;
			vector<idx_t> correlated_offsets;
			for (idx_t i = 0; i < count; i++) {
				correlated_bindings.emplace_back(base_binding.table_index,
				                                 ProjectionIndex(base_binding.column_index + i));
				correlated_offsets.push_back(correlated_offset + i);
			}
			return CorrelatedLayout(std::move(correlated_bindings), std::move(correlated_offsets));
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
		PushDownResult(unique_ptr<LogicalOperator> plan_p, CorrelatedLayout layout_p)
		    : plan(std::move(plan_p)), layout(std::move(layout_p)) {
		}

		unique_ptr<LogicalOperator> plan;
		CorrelatedLayout layout;
	};

	FlattenDependentJoins(Binder &binder, const CorrelatedColumns &correlated, bool perform_delim = true,
	                      bool any_join = false, optional_ptr<FlattenDependentJoins> parent = nullptr);

	static unique_ptr<LogicalOperator> DecorrelateIndependent(Binder &binder, unique_ptr<LogicalOperator> plan);

	PushDownResult Decorrelate(unique_ptr<LogicalOperator> plan, PushDownContext context = PushDownContext(),
	                           CorrelatedLayout layout = CorrelatedLayout());

private:
	//! Detects which Logical Operators have correlated expressions that they are dependent upon, filling the
	//! has_correlated_expressions map.
	bool DetectCorrelatedExpressions(LogicalOperator &op, bool lateral = false, idx_t lateral_depth = 0,
	                                 bool parent_is_dependent_join = false);

	//! Mark entire subtree of Logical Operators as correlated by adding them to the has_correlated_expressions map.
	bool MarkSubtreeCorrelated(LogicalOperator &op, TableIndex cte_index);

	//! Push the dependent join down a LogicalOperator
	PushDownResult PushDownDependentJoin(unique_ptr<LogicalOperator> plan, PushDownContext context = PushDownContext(),
	                                     CorrelatedLayout layout = CorrelatedLayout());
	PushDownResult DecorrelateDependentJoin(unique_ptr<LogicalOperator> plan, PushDownContext context,
	                                        CorrelatedLayout layout);

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
	CorrelatedLayout CreateCorrelatedLayout(TableIndex table_index, idx_t binding_offset,
	                                        idx_t correlated_offset) const;
	CorrelatedLayout CreateCorrelatedLayout(TableIndex table_index, idx_t correlated_offset) const;
	CorrelatedLayout CreateLeadingCorrelatedLayout(const vector<ColumnBinding> &bindings) const;
	void AppendCorrelatedColumns(vector<unique_ptr<Expression>> &expressions, const CorrelatedLayout &layout,
	                             idx_t count, bool include_names) const;
	void AppendCorrelatedColumnsToExpressionGet(LogicalExpressionGet &expr_get, const CorrelatedLayout &layout) const;
	void AddCorrelatedGroupColumns(LogicalAggregate &aggr, const CorrelatedLayout &layout, idx_t group_count) const;
	void AddCorrelatedFirstAggregates(LogicalAggregate &aggr, const CorrelatedLayout &layout) const;
	void AddAnyJoinConditions(LogicalDependentJoin &op, const vector<ColumnBinding> &plan_columns) const;
	void AddComparisonJoinConditions(LogicalComparisonJoin &join, const CorrelatedLayout &left_layout,
	                                 const CorrelatedLayout &right_layout) const;
	void AddCorrelatedJoinConditions(LogicalJoin &join, const CorrelatedLayout &left_layout,
	                                 const CorrelatedLayout &right_layout) const;
	void RewriteCorrelatedOperator(LogicalOperator &op, const CorrelatedLayout &layout, idx_t lateral_depth,
	                               bool recursive = false);
	CorrelatedLayout PrepareDependentJoinLeft(LogicalDependentJoin &op, PushDownContext context,
	                                          CorrelatedLayout layout);
	PushDownResult FinalizeDependentJoin(unique_ptr<LogicalOperator> plan, CorrelatedLayout layout,
	                                     const CorrelatedLayout &right_layout, idx_t lateral_depth);
	PushDownResult PushDownSingleCorrelatedChild(unique_ptr<LogicalOperator> plan, PushDownContext context,
	                                             CorrelatedLayout layout, bool correlated_left);
	CorrelatedLayout PushDownChild(unique_ptr<LogicalOperator> &child, const PushDownContext &context,
	                               CorrelatedLayout layout);
	CorrelatedLayout PushDownChildFresh(unique_ptr<LogicalOperator> &child, const PushDownContext &context,
	                                    CorrelatedLayout layout);
	PushDownResult PushDownFilter(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout);
	PushDownResult PushDownProjection(unique_ptr<LogicalOperator> plan, PushDownContext context,
	                                  CorrelatedLayout layout, bool exit_projection,
	                                  unique_ptr<LogicalOperator> delim_scan);
	PushDownResult PushDownAggregate(unique_ptr<LogicalOperator> plan, PushDownContext context,
	                                 CorrelatedLayout layout);
	PushDownResult PushDownCrossProduct(unique_ptr<LogicalOperator> plan, PushDownContext context,
	                                    CorrelatedLayout layout);
	PushDownResult PushDownJoin(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout);
	PushDownResult PushDownLimit(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout);
	PushDownResult PushDownWindow(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout);
	PushDownResult PushDownSetOperation(unique_ptr<LogicalOperator> plan, PushDownContext context,
	                                    CorrelatedLayout layout);
	PushDownResult PushDownDistinct(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout);
	PushDownResult PushDownExpressionGet(unique_ptr<LogicalOperator> plan, PushDownContext context,
	                                     CorrelatedLayout layout);
	PushDownResult PushDownOrderBy(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout);
	PushDownResult PushDownGet(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout);
	PushDownResult PushDownCTE(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout);
	PushDownResult PushDownCTERef(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout);
	PushDownResult PushDownDependentJoinInternal(unique_ptr<LogicalOperator> plan, PushDownContext context,
	                                             CorrelatedLayout layout);
};

} // namespace duckdb
