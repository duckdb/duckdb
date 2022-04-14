//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_piecewise_merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/common/sort/sort.hpp"

namespace duckdb {

struct GlobalSortState;

//! PhysicalRangeJoin represents one or more inequality range join predicates between
//! two tables
class PhysicalRangeJoin : public PhysicalComparisonJoin {
public:
	class LocalSortedTable {
	public:
		LocalSortedTable(const PhysicalRangeJoin &op, const idx_t child);

		void Sink(DataChunk &input, GlobalSortState &global_sort_state);

		inline void Sort(GlobalSortState &global_sort_state) {
			local_sort_state.Sort(global_sort_state, true);
		}

		//! The hosting operator
		const PhysicalRangeJoin &op;
		//! The local sort state
		LocalSortState local_sort_state;
		//! Local copy of the sorting expression executor
		ExpressionExecutor executor;
		//! Holds a vector of incoming sorting columns
		DataChunk keys;
		//! The number of NULL values
		idx_t has_null;
		//! The total number of rows
		idx_t count;

	private:
		// Merge the NULLs of all non-DISTINCT predicates into the primary so they sort to the end.
		idx_t MergeNulls(const vector<JoinCondition> &conditions);
	};

	class GlobalSortedTable {
	public:
		GlobalSortedTable(ClientContext &context, const vector<BoundOrderByNode> &orders, RowLayout &payload_layout);

		inline idx_t Count() const {
			return count;
		}

		inline idx_t BlockCount() const {
			if (global_sort_state.sorted_blocks.empty()) {
				return 0;
			}
			D_ASSERT(global_sort_state.sorted_blocks.size() == 1);
			return global_sort_state.sorted_blocks[0]->radix_sorting_data.size();
		}

		inline idx_t BlockSize(idx_t i) const {
			return global_sort_state.sorted_blocks[0]->radix_sorting_data[i].count;
		}

		void Combine(LocalSortedTable &ltable);
		void IntializeMatches();
		void Print();

		//! Starts the sorting process.
		void Finalize(Pipeline &pipeline, Event &event);
		//! Schedules tasks to merge sort the current child's data during a Finalize phase
		void ScheduleMergeTasks(Pipeline &pipeline, Event &event);

		GlobalSortState global_sort_state;
		//! Whether or not the RHS has NULL values
		atomic<idx_t> has_null;
		//! The total number of rows in the RHS
		atomic<idx_t> count;
		//! A bool indicating for each tuple in the RHS if they found a match (only used in FULL OUTER JOIN)
		unique_ptr<bool[]> found_match;
		//! Memory usage per thread
		idx_t memory_per_thread;
	};

public:
	PhysicalRangeJoin(LogicalOperator &op, PhysicalOperatorType type, unique_ptr<PhysicalOperator> left,
	                  unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
	                  idx_t estimated_cardinality);

public:
	// Gather the result values and slice the payload columns to those values.
	static void SliceSortedPayload(DataChunk &payload, GlobalSortState &state, const idx_t block_idx,
	                               const SelectionVector &result, const idx_t result_count, const idx_t left_cols = 0);
	// Apply a tail condition to the current selection
	static idx_t SelectJoinTail(const ExpressionType &condition, Vector &left, Vector &right,
	                            const SelectionVector *sel, idx_t count, SelectionVector *true_sel);
};

} // namespace duckdb
