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

class DecorrelationStateCollector;

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
	struct CorrelatedLayout {
		static CorrelatedLayout Initial(const CorrelatedColumns &correlated_columns) {
			return CorrelatedLayout(correlated_columns, {}, {}, true);
		}

		static CorrelatedLayout CreateContiguous(const CorrelatedColumns &correlated_columns,
		                                         ColumnBinding base_binding, idx_t correlated_offset) {
			auto count = correlated_columns.size();
			vector<ColumnBinding> correlated_bindings;
			vector<idx_t> correlated_offsets;
			correlated_bindings.reserve(count);
			correlated_offsets.reserve(count);
			for (idx_t i = 0; i < count; i++) {
				correlated_bindings.emplace_back(base_binding.table_index,
				                                 ProjectionIndex(base_binding.column_index + i));
				correlated_offsets.push_back(correlated_offset + i);
			}
			return CorrelatedLayout(correlated_columns, std::move(correlated_bindings), std::move(correlated_offsets));
		}

		static CorrelatedLayout CreateLeading(const CorrelatedColumns &correlated_columns,
		                                      const vector<ColumnBinding> &bindings) {
			auto count = correlated_columns.size();
			D_ASSERT(bindings.size() >= count);
			vector<ColumnBinding> correlated_bindings;
			vector<idx_t> correlated_offsets;
			correlated_bindings.reserve(count);
			correlated_offsets.reserve(count);
			for (idx_t i = 0; i < count; i++) {
				correlated_bindings.push_back(bindings[i]);
				correlated_offsets.push_back(i);
			}
			return CorrelatedLayout(correlated_columns, std::move(correlated_bindings), std::move(correlated_offsets));
		}

		idx_t size() const {
			AssertUsable();
			return correlated_bindings.size();
		}

		const vector<ColumnBinding> &GetBindings() const {
			AssertUsable();
			return correlated_bindings;
		}

		const CorrelatedColumnInfo &GetColumn(idx_t index) const {
			AssertUsable();
			return correlated_columns.get()[index];
		}

		idx_t GetDelimKeyCount(bool perform_delim) const {
			return perform_delim ? size() : 1;
		}

		const CorrelatedColumnInfo &GetDelimKey(idx_t index, bool perform_delim) const {
			return GetColumn(GetDelimKeyIndex(index, perform_delim));
		}

		const ColumnBinding &GetBinding(idx_t index) const {
			AssertUsable();
			D_ASSERT(index < correlated_bindings.size());
			return correlated_bindings[index];
		}

		const ColumnBinding &GetDelimBinding(idx_t index, bool perform_delim) const {
			return GetBinding(GetDelimKeyIndex(index, perform_delim));
		}

		idx_t GetOffset(idx_t index) const {
			AssertUsable();
			D_ASSERT(index < correlated_offsets.size());
			return correlated_offsets[index];
		}

		idx_t GetDelimOffset(idx_t index, bool perform_delim) const {
			return GetOffset(GetDelimKeyIndex(index, perform_delim));
		}

		void ShiftOffsets(idx_t offset) {
			AssertUsable();
			for (auto &entry : correlated_offsets) {
				entry += offset;
			}
		}

		void ResetContiguousOffsets(idx_t offset) {
			AssertUsable();
			D_ASSERT(correlated_offsets.size() == correlated_bindings.size());
			for (idx_t i = 0; i < correlated_offsets.size(); i++) {
				correlated_offsets[i] = offset + i;
			}
		}

	private:
		void AssertUsable() const {
			D_ASSERT(!is_initial);
		}

		idx_t GetDelimKeyIndex(idx_t index, bool perform_delim) const {
			D_ASSERT(index < GetDelimKeyCount(perform_delim));
			if (perform_delim) {
				return index;
			}
			auto delim_index = correlated_columns.get().GetDelimIndex();
			D_ASSERT(delim_index < correlated_columns.get().size());
			return delim_index;
		}

		CorrelatedLayout(const CorrelatedColumns &correlated_columns_p, vector<ColumnBinding> correlated_bindings_p,
		                 vector<idx_t> correlated_offsets_p, bool is_initial_p = false)
		    : correlated_columns(correlated_columns_p), correlated_bindings(std::move(correlated_bindings_p)),
		      correlated_offsets(std::move(correlated_offsets_p)), is_initial(is_initial_p) {
			D_ASSERT(correlated_bindings.size() == correlated_offsets.size());
		}

		const_reference<CorrelatedColumns> correlated_columns;
		vector<ColumnBinding> correlated_bindings;
		vector<idx_t> correlated_offsets;
		bool is_initial;
	};

	struct PushDownResult {
		PushDownResult(unique_ptr<LogicalOperator> plan_p, CorrelatedLayout layout_p)
		    : plan(std::move(plan_p)), layout(std::move(layout_p)) {
		}

		unique_ptr<LogicalOperator> plan;
		CorrelatedLayout layout;
	};

public:
	class DecorrelationState {
	public:
		void Collect(Binder &binder, LogicalOperator &op);
		bool IsCollected(LogicalOperator &op) const;
		bool DependsOnCorrelated(LogicalOperator &op, const column_binding_map_t<idx_t> &correlated_map) const;
		column_binding_set_t &DependenciesFor(LogicalOperator &op);
		optional_ptr<const reference_set_t<LogicalOperator>> AccessorsFor(LogicalOperator &op,
		                                                                  TableIndex table_index) const;
		bool HasAccessorsFor(LogicalOperator &op, TableIndex table_index) const;

	private:
		reference_map_t<LogicalOperator, column_binding_set_t> subtree_dependencies;
		reference_map_t<LogicalOperator, unordered_map<TableIndex, reference_set_t<LogicalOperator>>> subtree_accessors;

		friend class DecorrelationStateCollector;
	};

private:
	FlattenDependentJoins(Binder &binder, const CorrelatedColumns &correlated, bool perform_delim = true,
	                      bool any_join = false, optional_ptr<FlattenDependentJoins> parent = nullptr);

	PushDownResult Decorrelate(unique_ptr<LogicalOperator> plan) {
		return Decorrelate(std::move(plan), true, CorrelatedLayout::Initial(correlated_columns));
	}
	PushDownResult Decorrelate(unique_ptr<LogicalOperator> plan, bool propagate_null_values, CorrelatedLayout layout);
	static void CreateDelimJoinConditions(LogicalComparisonJoin &delim_join, vector<ColumnBinding> bindings,
	                                      const CorrelatedLayout &layout, bool perform_delim);
	//! Detects which Logical Operators have correlated expressions that they are dependent upon, filling the
	//! decorrelation state.
	void CollectDecorrelationState(LogicalOperator &op);
	bool DependsOnCorrelated(LogicalOperator &op) const;

	//! Push the dependent join down a LogicalOperator
	PushDownResult PushDownDependentJoin(unique_ptr<LogicalOperator> plan, bool propagate_null_values = true) {
		return PushDownDependentJoin(std::move(plan), propagate_null_values,
		                             CorrelatedLayout::Initial(correlated_columns));
	}
	PushDownResult PushDownDependentJoin(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                                     CorrelatedLayout layout);
	PushDownResult DecorrelateDependentJoin(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                                        CorrelatedLayout layout);
	DecorrelationState &GetDecorrelationState(LogicalOperator &op);
	Binder &binder;
	column_binding_map_t<idx_t> correlated_map;
	column_binding_map_t<idx_t> replacement_map;
	const CorrelatedColumns &correlated_columns;
	vector<LogicalType> delim_types;

	bool perform_delim;
	bool any_join;
	optional_ptr<FlattenDependentJoins> parent;
	shared_ptr<DecorrelationState> decorrelation_state;
	void AppendDelimColumns(vector<unique_ptr<Expression>> &expressions, const CorrelatedLayout &layout,
	                        bool include_names) const;
	void AppendCorrelatedColumns(vector<unique_ptr<Expression>> &expressions, const CorrelatedLayout &layout,
	                             idx_t count, bool include_names) const;
	void AddDelimColumnsToGroup(LogicalAggregate &aggr, const CorrelatedLayout &layout) const;
	void AddCorrelatedFirstAggregates(LogicalAggregate &aggr, const CorrelatedLayout &layout) const;
	void AddAnyJoinConditions(LogicalDependentJoin &op, const vector<ColumnBinding> &plan_columns) const;
	static vector<ColumnBinding> GetDependentJoinPlanColumns(LogicalOperator &op);
	static void PopulateDuplicateEliminatedColumns(LogicalDependentJoin &op);
	void AddComparisonJoinConditions(LogicalComparisonJoin &join, const CorrelatedLayout &left_layout,
	                                 const CorrelatedLayout &right_layout) const;
	void AddCTERefJoinConditions(LogicalComparisonJoin &join, const LogicalCTERef &cteref,
	                             const CorrelatedLayout &layout) const;
	void AddCorrelatedJoinConditions(LogicalJoin &join, const CorrelatedLayout &left_layout,
	                                 const CorrelatedLayout &right_layout) const;
	void PatchAccessingOperators(LogicalOperator &subtree_root, TableIndex table_index,
	                             const CorrelatedColumns &correlated_columns);
	CorrelatedLayout PrepareDependentJoinLeft(LogicalDependentJoin &op, bool propagate_null_values,
	                                          CorrelatedLayout layout);
	PushDownResult FinalizeDependentJoin(unique_ptr<LogicalOperator> plan, CorrelatedLayout outer_layout,
	                                     CorrelatedLayout right_layout);
	PushDownResult PushDownSingleCorrelatedChild(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                                             CorrelatedLayout layout, bool correlated_left);
	CorrelatedLayout PushDownChild(unique_ptr<LogicalOperator> &child, bool propagate_null_values,
	                               CorrelatedLayout layout, bool fresh = false);
	PushDownResult PushDownFilter(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                              CorrelatedLayout layout);
	PushDownResult PushDownProjection(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                                  CorrelatedLayout layout);
	PushDownResult FinalizeProjection(unique_ptr<LogicalOperator> plan, CorrelatedLayout layout,
	                                  const vector<ColumnBinding> &old_child_bindings);
	PushDownResult PushDownAggregate(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                                 CorrelatedLayout layout);
	PushDownResult PushDownCrossProduct(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                                    CorrelatedLayout layout);
	PushDownResult PushDownJoin(unique_ptr<LogicalOperator> plan, bool propagate_null_values, CorrelatedLayout layout);
	PushDownResult PushDownLimit(unique_ptr<LogicalOperator> plan, bool propagate_null_values, CorrelatedLayout layout);
	PushDownResult PushDownWindow(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                              CorrelatedLayout layout);
	PushDownResult PushDownSetOperation(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                                    CorrelatedLayout layout);
	PushDownResult PushDownDistinct(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                                CorrelatedLayout layout);
	PushDownResult PushDownExpressionGet(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                                     CorrelatedLayout layout);
	PushDownResult PushDownOrderBy(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                               CorrelatedLayout layout);
	PushDownResult PushDownGet(unique_ptr<LogicalOperator> plan, bool propagate_null_values, CorrelatedLayout layout);
	PushDownResult PushDownCTE(unique_ptr<LogicalOperator> plan, bool propagate_null_values, CorrelatedLayout layout);
	PushDownResult PushDownCTERef(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                              CorrelatedLayout layout);
	PushDownResult PushDownDependentJoinInternal(unique_ptr<LogicalOperator> plan, bool propagate_null_values,
	                                             CorrelatedLayout layout);
};

} // namespace duckdb
