// This is a custom operator to perform an overlap join in DuckDb.
// It is a specialized join which involves creating a sweepline index to do overlap joins on time-series data.
// we are basing this off of src/execution/operator/join/physical_range_join.cpp
// This code will be performance and parallel thread-safety in mind
// but we will not be registering the logical operator yet.

#include "duckdb/execution/operator/join/physical_overlap_join.hpp"

#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/comparators.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/executor_task.hpp"

#include <thread>

#include "duckdb/execution/operator/join/physical_overlap_join.hpp"

// Similar includes as in range join
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
// ... other includes

namespace duckdb {

// Sweepline index class for efficient overlap detection
SweeplineIndex::SweeplineIndex() {
    // use sorted vectors
    // standard vector is fine, cache is good
    // don't need to worry about thread-safety for now since it is constructed after
        // This is made in Global Sort Table - singular thread construction for right side
}

void SweeplineIndex::AddInterval(idx_t row_id, timestamp_t start, timestamp_t end) {
    // Add interval to the index
    intervals.push_back({row_id, start, end});

    // add the events to the index
        // I need to know when it starts and ends for this specific row
    events.push_back({start, true, intervals.size() - 1});
    events.push_back({end, false, intervals.size() - 1});
}

// Needs to be used!!!e
    // DuckDB > Sort > Local > Global > Create Index(right) > Prepare(Sort) > Run overlap
void SweeplineIndex::Prepare() {
    // Prepare the index for searching
    // Sort intervals by start time
        // currently just use this generic sort
    sort(intervals.begin(), intervals.end(), [](const Interval &a, const Interval &b) {
        return a.start < b.start;
    });
}

vector<idx_t> SweeplineIndex::FindOverlaps(timestamp_t q_start, timestamp_t q_end) {
    // Find overlapping intervals
    vector<idx_t> results;
    active_intervals.clear();

    // set the method call to the last event call
    size_t event_idx = current_event_idx;
    while (event_idx < events.size() && events[event_idx].time < q_start) {
        // If this is a start event, add the interval to active set
        if (events[event_idx].is_start) {
            active_intervals.insert(events[event_idx].interval_idx);
        } else {
            // Remove from active set (implementation depends on your data structure)
            active_intervals.erase(events[event_idx].interval_idx);
        }
        event_idx++;
    }
    
    // Check intervals that are already active when we reach query_start
    for (idx_t interval_idx : active_intervals) {
        const Interval& interval = intervals[interval_idx];
        // These intervals started before query_start and are still active
        // They overlap with the query if they end after query_start
        if (interval.end >= q_start) {
            results.push_back(interval.row_id);
        }
    }
    
    // Continue processing events until query_end
    while (event_idx < events.size() && events[event_idx].time <= q_end) {
        const Event& event = events[event_idx];
        
        if (event.is_start) {
            // New interval becomes active
            active_intervals.insert(event.interval_idx);
            
            // Since it starts during our query range, if it doesn't end before
            // query_start, it's an overlap
            const Interval& interval = intervals[event.interval_idx];
            if (interval.end >= q_start) {
                results.push_back(interval.row_id);
            }
        } else {
            // Interval is no longer active
            active_intervals.erase(event.interval_idx);
            // No need to check for overlaps here as we've already added this interval
            // if it was relevant
        }
        
        event_idx++;
    }
    return results;
}

//===--------------------------------------------------------------------===//
// LocalSortedTable Implementation
//===--------------------------------------------------------------------===//
//-- 123
PhysicalOverlapJoin::LocalSortedTable::LocalSortedTable(ClientContext &context, const PhysicalOverlapJoin &op,
                                                      const idx_t child)
    : op(op), executor(context), has_null(0), count(0) {
	// Initialize order clause expression executor and key DataChunk
	vector<LogicalType> types;
	for (const auto &cond : op.conditions) {
		const auto &expr = child ? cond.right : cond.left;
		executor.AddExpression(*expr);

		types.push_back(expr->return_type);
	}
	auto &allocator = Allocator::Get(context);
	keys.Initialize(allocator, types);
}

// -- 123
    // Sink can be almost identical - what about sorting by timestamp

    // local table sink
void PhysicalOverlapJoin::LocalSortedTable::Sink(DataChunk &input, GlobalSortState &global_sort_state) {
	// Initialize local state (if necessary)
        // lazy initialization
	if (!local_sort_state.initialized) {
		local_sort_state.Initialize(global_sort_state, global_sort_state.buffer_manager);
	}

	// Obtain sorting columns
	keys.Reset();
    // execute goes and fills the keys based on the input from SQL after the parser
        // no need to change this - will automatically identify the time columns (start,end)
	executor.Execute(input, keys);

	// Do not operate on primary key directly to avoid modifying the input chunk
	Vector primary = keys.data[0];
	// Count the NULLs so we can exclude them later
	has_null += MergeNulls(primary, op.conditions);
	count += keys.size();

	//	Only sort the primary key
	DataChunk join_head;
	join_head.data.emplace_back(primary);
	join_head.SetCardinality(keys.size());

	// Sink the data into the local sort state
	local_sort_state.SinkChunk(join_head, input);
}

// -- 987
    // need to check on this one
idx_t PhysicalOverlapJoin::LocalSortedTable::MergeNulls(Vector &primary, const vector<JoinCondition> &conditions) {
    // Implementation of MergeNulls method
        // don't know if this is required
    return 0; // Placeholder
}

//===--------------------------------------------------------------------===//
// GlobalSortedTable Implementation
//===--------------------------------------------------------------------===//
PhysicalOverlapJoin::GlobalSortedTable::GlobalSortedTable(ClientContext &context, const vector<BoundOrderByNode> &orders,
                                                       RowLayout &payload_layout, const PhysicalOperator &op_p)
    : op(op_p), global_sort_state(BufferManager::GetBufferManager(context), orders, payload_layout),
      count(0), memory_per_thread(0) {
    // Constructor implementation
}

PhysicalOverlapJoin::GlobalSortedTable::GlobalSortedTable(ClientContext &context, const vector<BoundOrderByNode> &orders,
                                                        RowLayout &payload_layout, const PhysicalOperator &op_p)
    : op(op_p), global_sort_state(BufferManager::GetBufferManager(context), orders, payload_layout), has_null(0),
      count(0), memory_per_thread(0) {

	// Set external (can be forced with the PRAGMA)
	auto &config = ClientConfig::GetConfig(context);
	global_sort_state.external = config.force_external;
	memory_per_thread = PhysicalOverlapJoin::GetMaxThreadMemory(context);
}


void PhysicalOverlapJoin::GlobalSortedTable::Combine(LocalSortedTable &ltable) {
	global_sort_state.AddLocalState(ltable.local_sort_state);
	has_null += ltable.has_null;
	count += ltable.count;
}

void PhysicalOverlapJoin::GlobalSortedTable::BuildSweeplineIndex() {
    // Implementation of BuildSweeplineIndex
        // make_unique is deprecated, use make_uniq
    sweep_index = make_uniq<SweeplineIndex>();
    
    // Build the index from sorted data
}

// outputs the current state
void PhysicalOverlapJoin::GlobalSortedTable::Print() {
    global_sort_state.Print();
}

//===--------------------------------------------------------------------===//
// Task Classes for Parallel Processing of the GLOBAL SORT/MERGE -- NOT OPERATOR
//===--------------------------------------------------------------------===//
class OverlapJoinMergeTask : public ExecutorTask {
public:
    using GlobalSortedTable = PhysicalOverlapJoin::GlobalSortedTable;

public:
    OverlapJoinMergeTask(shared_ptr<Event> event_p, ClientContext &context, GlobalSortedTable &table)
        : ExecutorTask(context, std::move(event_p), table.op), context(context), table(table) {
    }

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		// Initialize iejoin sorted and iterate until done
            // this is just sorting
            // no merge is happenig here yet - prepping global sort table
		auto &global_sort_state = table.global_sort_state;
		MergeSorter merge_sorter(global_sort_state, BufferManager::GetBufferManager(context));
		merge_sorter.PerformInMergeRound();
		event->FinishTask();

		return TaskExecutionResult::TASK_FINISHED;
	}
private:
    ClientContext &context;
    GlobalSortedTable &table;
};

class OverlapJoinMergeEvent : public BasePipelineEvent {
public:
    using GlobalSortedTable = PhysicalOverlapJoin::GlobalSortedTable;

public:
    OverlapJoinMergeEvent(GlobalSortedTable &table_p, Pipeline &pipeline_p)
        : BasePipelineEvent(pipeline_p), table(table_p) {
    }

    GlobalSortedTable &table;

public:
    void Schedule() override {
        auto &context = pipeline->GetClientContext();

        // Schedule tasks equal to the number of threads, which will each merge multiple partitions
        auto &ts = TaskScheduler::GetScheduler(context);
        auto num_threads = NumericCast<idx_t>(ts.NumberOfThreads());

        vector<shared_ptr<Task>> iejoin_tasks;
        for (idx_t tnum = 0; tnum < num_threads; tnum++) {
            iejoin_tasks.push_back(make_uniq<OverlapJoinMergeTask>(shared_from_this(), context, table));
        }
        SetTasks(std::move(iejoin_tasks));
    }

    void FinishEvent() override {
		auto &global_sort_state = table.global_sort_state;

		global_sort_state.CompleteMergeRound(true);
		if (global_sort_state.sorted_blocks.size() > 1) {
			// Multiple blocks remaining: Schedule the next round
			table.ScheduleMergeTasks(*pipeline, *this);
		}
    }
};

// this is just the global merge, no operator merge
void PhysicalOverlapJoin::GlobalSortedTable::ScheduleMergeTasks(Pipeline &pipeline, Event &event) {
    // init global sort state for merge
	global_sort_state.InitializeMergeRound();
	auto new_event = make_shared_ptr<OverlapJoinMergeEvent>(*this, pipeline);
	event.InsertEvent(std::move(new_event));
}

// this is just global sort finalize, not operator finalize
void PhysicalOverlapJoin::GlobalSortedTable::Finalize(Pipeline &pipeline, Event &event) {
	// Prepare for merge sort phase
        // THIS IS GLOBALSORTTABLE FINALIZE - NOT JOIN FINALIZE
	global_sort_state.PrepareMergePhase();

	// Start the merge phase or finish if a merge is not necessary
	if (global_sort_state.sorted_blocks.size() > 1) {
		ScheduleMergeTasks(pipeline, event);
	}
}

//===--------------------------------------------------------------------===//
// PhysicalOverlapJoin Implementation
//===--------------------------------------------------------------------===//

// !!! TODO: !!!
PhysicalOverlapJoin::PhysicalOverlapJoin(LogicalComparisonJoin &op, unique_ptr<PhysicalOperator> left,
                                       unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                       JoinType join_type, idx_t estimated_cardinality)
    : PhysicalComparisonJoin(
        op, 
        type, 
        std::move(cond), 
        join_type, 
        estimated_cardinality) 
{
            // constructor - should I be using COMPARISON_JOIN or just JOIN?

    // join keys
    for (auto &condition : conditions) {
        join_key_types.push_back(condition.left->return_type);
    }

    // --- Set up sort orders: only sort by start time (assume it's first join condition)
    D_ASSERT(conditions.size() >= 2);
    // Left input: sort by left.start
    lhs_order.expression = conditions[0].left->Copy();
    lhs_order.order_type = OrderType::ASCENDING;
    lhs_orders.push_back(std::move(lhs_order));

    // Right input: sort by right.start
    rhs_order.expression = conditions[1].right->Copy();
    rhs_order.order_type = OrderType::ASCENDING;
    rhs_orders.push_back(std::move(rhs_order));
}

BufferHandle PhysicalOverlapJoin::SliceSortedPayload(DataChunk &payload, GlobalSortState &state, const idx_t block_idx,
                                                   const SelectionVector &result, const idx_t result_count,
                                                   const idx_t left_cols) {
	D_ASSERT(state.sorted_blocks.size() == 1);
	SBScanState read_state(state.buffer_manager, state);
	read_state.sb = state.sorted_blocks[0].get();
	auto &sorted_data = *read_state.sb->payload_data;

	read_state.SetIndices(block_idx, 0);
	read_state.PinData(sorted_data);
	const auto data_ptr = read_state.DataPtr(sorted_data);
	data_ptr_t heap_ptr = nullptr;

	// Set up a batch of pointers to scan data from
	Vector addresses(LogicalType::POINTER, result_count);
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	// Set up the data pointers for the values that are actually referenced
	const idx_t &row_width = sorted_data.layout.GetRowWidth();

	auto prev_idx = result.get_index(0);
	SelectionVector gsel(result_count);
	idx_t addr_count = 0;
	gsel.set_index(0, addr_count);
	data_pointers[addr_count] = data_ptr + prev_idx * row_width;
	for (idx_t i = 1; i < result_count; ++i) {
		const auto row_idx = result.get_index(i);
		if (row_idx != prev_idx) {
			data_pointers[++addr_count] = data_ptr + row_idx * row_width;
			prev_idx = row_idx;
		}
		gsel.set_index(i, addr_count);
	}
	++addr_count;

	// Unswizzle the offsets back to pointers (if needed)
	if (!sorted_data.layout.AllConstant() && state.external) {
		heap_ptr = read_state.payload_heap_handle.Ptr();
	}

	// Deserialize the payload data
	auto sel = FlatVector::IncrementalSelectionVector();
	for (idx_t col_no = 0; col_no < sorted_data.layout.ColumnCount(); col_no++) {
		auto &col = payload.data[left_cols + col_no];
		RowOperations::Gather(addresses, *sel, col, *sel, addr_count, sorted_data.layout, col_no, 0, heap_ptr);
		col.Slice(gsel, result_count);
	}

	return std::move(read_state.payload_heap_handle);	D_ASSERT(state.sorted_blocks.size() == 1);
	SBScanState read_state(state.buffer_manager, state);
	read_state.sb = state.sorted_blocks[0].get();
	auto &sorted_data = *read_state.sb->payload_data;

	read_state.SetIndices(block_idx, 0);
	read_state.PinData(sorted_data);
	const auto data_ptr = read_state.DataPtr(sorted_data);
	data_ptr_t heap_ptr = nullptr;

	// Set up a batch of pointers to scan data from
	Vector addresses(LogicalType::POINTER, result_count);
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	// Set up the data pointers for the values that are actually referenced
	const idx_t &row_width = sorted_data.layout.GetRowWidth();

	auto prev_idx = result.get_index(0);
	SelectionVector gsel(result_count);
	idx_t addr_count = 0;
	gsel.set_index(0, addr_count);
	data_pointers[addr_count] = data_ptr + prev_idx * row_width;
	for (idx_t i = 1; i < result_count; ++i) {
		const auto row_idx = result.get_index(i);
		if (row_idx != prev_idx) {
			data_pointers[++addr_count] = data_ptr + row_idx * row_width;
			prev_idx = row_idx;
		}
		gsel.set_index(i, addr_count);
	}
	++addr_count;

	// Unswizzle the offsets back to pointers (if needed)
	if (!sorted_data.layout.AllConstant() && state.external) {
		heap_ptr = read_state.payload_heap_handle.Ptr();
	}

	// Deserialize the payload data
	auto sel = FlatVector::IncrementalSelectionVector();
	for (idx_t col_no = 0; col_no < sorted_data.layout.ColumnCount(); col_no++) {
		auto &col = payload.data[left_cols + col_no];
		RowOperations::Gather(addresses, *sel, col, *sel, addr_count, sorted_data.layout, col_no, 0, heap_ptr);
		col.Slice(gsel, result_count);
	}

    // BufferHandle
	return std::move(read_state.payload_heap_handle);
    //return BufferHandle();
}

// !!! TODO: !!!
void PhysicalOverlapJoin::FindOverlaps(GlobalSortedTable &left, GlobalSortedTable &right,
                                      vector<pair<idx_t, idx_t>> &result_pairs) const {
    // Implementation of FindOverlaps for left and right side of Sorted Table
}

// should be no change since the projection is not overlap join specific
void PhysicalOverlapJoin::ProjectResult(DataChunk &chunk, DataChunk &result) const {
	const auto left_projected = left_projection_map.size();
	for (idx_t i = 0; i < left_projected; ++i) {
		result.data[i].Reference(chunk.data[left_projection_map[i]]);
	}
	const auto left_width = children[0]->types.size();
	for (idx_t i = 0; i < right_projection_map.size(); ++i) {
		result.data[left_projected + i].Reference(chunk.data[left_width + right_projection_map[i]]);
	}
	result.SetCardinality(chunk);
}

//===--------------------------------------------------------------------===//
    // Sink
//===--------------------------------------------------------------------===//

// reuse
class OverlapJoinLocalState : public LocalSinkState {
    public:
        using LocalSortedTable = PhysicalOverlapJoin::LocalSortedTable;
    
        OverlapJoinLocalState(ClientContext &context, const PhysicalOverlapJoin &op, const idx_t child)
            : table(context, op, child) {
        }
    
        //! The local sort state
        LocalSortedTable table;
    };

class OverlapGlobalState : public GlobalSinkState {
    public:
        // borrow from range join's global state
        using GlobalSortedTable = PhysicalOverlapJoin::GlobalSortedTable;
    
    public:
        // table[0] is left and table[1] is right
        OverlapGlobalState(ClientContext &context, const PhysicalOverlapJoin &op) : child(0) {
            tables.resize(2);
            RowLayout lhs_layout;
            lhs_layout.Initialize(op.children[0]->types);
            vector<BoundOrderByNode> lhs_order;
            lhs_order.emplace_back(op.lhs_orders[0].Copy());
            tables[0] = make_uniq<GlobalSortedTable>(context, lhs_order, lhs_layout, op);
    
            RowLayout rhs_layout;
            rhs_layout.Initialize(op.children[1]->types);
            vector<BoundOrderByNode> rhs_order;
            rhs_order.emplace_back(op.rhs_orders[0].Copy());
            tables[1] = make_uniq<GlobalSortedTable>(context, rhs_order, rhs_layout, op);
        }
    
        // this is for passing between different phases of iejoin
            // keep it for when processing different chunks

        OverlapGlobalState(OverlapGlobalState &prev) : tables(std::move(prev.tables)), child(prev.child + 1) {
            state = prev.state;
        }
    
        void Sink(DataChunk &input, OverlapJoinLocalState &lstate) {
            //auto &local_table = tables[child];
            auto &global_table = *tables[child];
            auto &global_sort_state = global_table.global_sort_state;
            auto &local_sort_state = lstate.table.local_sort_state;
    
            // Sink the data into the local sort state
            lstate.table.Sink(input, global_sort_state);
    
            // When sorting data reaches a certain size, we sort it
            if (local_sort_state.SizeInBytes() >= global_table.memory_per_thread) {
                local_sort_state.Sort(global_sort_state, true);
            }
        }
    
        // variables of GlobalSortedTable
        vector<unique_ptr<GlobalSortedTable>> tables;
        size_t child;
    };


unique_ptr<GlobalSinkState> PhysicalOverlapJoin::GetGlobalSinkState(ClientContext &context) const {
	D_ASSERT(!sink_state);
	return make_uniq<OverlapGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalOverlapJoin::GetLocalSinkState(ExecutionContext &context) const {
	idx_t sink_child = 0;
	if (sink_state) {
		const auto &ie_sink = sink_state->Cast<OverlapGlobalState>();
		sink_child = ie_sink.child;
	}
	return make_uniq<OverlapJoinLocalState>(context.client, *this, sink_child);
}

SinkResultType PhysicalOverlapJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<OverlapGlobalState>();
	auto &lstate = input.local_state.Cast<OverlapJoinLocalState>();

	gstate.Sink(chunk, lstate);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalOverlapJoin::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<OverlapGlobalState>();
	auto &lstate = input.local_state.Cast<OverlapJoinLocalState>();
	gstate.tables[gstate.child]->Combine(lstate.table);
	auto &client_profiler = QueryProfiler::Get(context.client);

	context.thread.profiler.Flush(*this);
	client_profiler.Flush(context.thread.profiler);

	return SinkCombineResultType::FINISHED;
}

} // namespace duckdb