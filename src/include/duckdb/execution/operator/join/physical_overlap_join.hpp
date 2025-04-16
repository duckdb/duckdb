//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_piecewise_merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

// this is the least required ones so far

#pragma once

#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/constants.hpp"
#include <unordered_set>

// Optional (only if explicitly needed)
// #include "duckdb/planner/bound_result_modifier.hpp"

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_overlap_join.hpp
//
//
//===----------------------------------------------------------------------===//

//#pragma once

#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/common/sort/sort.hpp"

namespace duckdb {

struct GlobalSortState;

//! SweeplineIndex represents an efficient data structure for finding
//! overlapping time intervals
class SweeplineIndex {
public:
    SweeplineIndex();
    
    void AddInterval(idx_t row_id, timestamp_t start, timestamp_t end);
    void Prepare();
    // need to make it thread-safe
    vector<idx_t> FindOverlaps(timestamp_t start, timestamp_t end, 
                         size_t &current_event_idx,
                         unordered_set<idx_t> &active_intervals);
    
private:
    struct Interval {
        idx_t row_id;
        timestamp_t start;
        timestamp_t end;
    };
    
    struct Event {
        timestamp_t time;
        bool is_start;
        idx_t interval_idx;
    };
    
    // this is my "struct" of sorted vectors
    vector<Interval> intervals;
    vector<Event> events;

    // these need to go to local thread
    /*
    size_t current_event_idx = 0;
    unordered_set<idx_t> active_intervals;
    */
};

//! PhysicalOverlapJoin represents a specialized join that efficiently finds
//! overlapping time intervals between two tables
class PhysicalOverlapJoin : public PhysicalComparisonJoin {
public:
    class LocalSortedTable {
    public:
        LocalSortedTable(ClientContext &context, const PhysicalOverlapJoin &op, const idx_t child);

        void Sink(DataChunk &input, GlobalSortState &global_sort_state);

        inline void Sort(GlobalSortState &global_sort_state) {
            local_sort_state.Sort(global_sort_state, true);
        }

        //! The hosting operator
        const PhysicalOverlapJoin &op;
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
        idx_t MergeNulls(Vector &primary, const vector<JoinCondition> &conditions);
    };

    class GlobalSortedTable {
    public:
        GlobalSortedTable(ClientContext &context, const vector<BoundOrderByNode> &orders, RowLayout &payload_layout,
                        const PhysicalOperator &op);

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
            return global_sort_state.sorted_blocks[0]->radix_sorting_data[i]->count;
        }

        void Combine(LocalSortedTable &ltable);
        void Print();

        //! Starts the sorting process.
        void Finalize(Pipeline &pipeline, Event &event);
        //! Schedules tasks to merge sort the current child's data during a Finalize phase
        void ScheduleMergeTasks(Pipeline &pipeline, Event &event);
        
        //! Build the sweepline index on sorted data
        void BuildSweeplineIndex();

        //! The hosting operator
        const PhysicalOperator &op;
        GlobalSortState global_sort_state;
        //! The total number of rows
        atomic<idx_t> count;
        //! Memory usage per thread
        idx_t memory_per_thread;
        //! Whether or not the RHS has NULL values
		atomic<idx_t> has_null;

        //--
        //! Sweepline index for efficient overlap detection
        //--
        unique_ptr<SweeplineIndex> sweep_index;


        //! A bool indicating for each tuple in the RHS if they found a match (only used in FULL OUTER JOIN)
            // overlap should almost never have full outer join
        /*unsafe_unique_array<bool> found_match;*/
    };

public:
    PhysicalOverlapJoin(LogicalComparisonJoin &op, unique_ptr<PhysicalOperator> left,
                      unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                      idx_t estimated_cardinality);

    // Projection mappings
    using ProjectionMapping = vector<column_t>;
    ProjectionMapping left_projection_map;
    ProjectionMapping right_projection_map;
    
    // do I need both?
    // The join key types for the left and right children
    vector<LogicalType> join_key_types;
	vector<BoundOrderByNode> lhs_orders;
	vector<BoundOrderByNode> rhs_orders;

// ----
    // carry over
// ----
public:
    // Gather the result values and slice the payload columns to those values.
    // Returns a buffer handle to the pinned heap block (if any)
    static BufferHandle SliceSortedPayload(DataChunk &payload, GlobalSortState &state, const idx_t block_idx,
                                         const SelectionVector &result, const idx_t result_count,
                                         const idx_t left_cols = 0);

    //! Find overlapping intervals using the sweepline algorithm
    void FindOverlaps(GlobalSortedTable &left, GlobalSortedTable &right, 
                     vector<pair<idx_t, idx_t>> &result_pairs) const;

    //! Utility to project full width internal chunks to projected results
    void ProjectResult(DataChunk &chunk, DataChunk &result) const;

public:
	// CachingOperator Interface
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;
public:
	// Source interface
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const override;

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

// this is how it works as a 2-phase (similar to ie_join)?                                        
public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;


};

} // namespace duckdb


