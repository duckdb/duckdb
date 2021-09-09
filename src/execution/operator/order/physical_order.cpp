#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t estimated_cardinality)
    : PhysicalSink(PhysicalOperatorType::ORDER_BY, move(types), estimated_cardinality), orders(move(orders)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class OrderGlobalState : public GlobalOperatorState {
public:
	OrderGlobalState(BufferManager &buffer_manager, PhysicalOrder &order, RowLayout &payload_layout)
	    : global_sort_state(buffer_manager, order.orders, payload_layout) {
	}

	//! Global sort state
	GlobalSortState global_sort_state;
	//! Memory usage per thread
	idx_t memory_per_thread;
};

class OrderLocalState : public LocalSinkState {
public:
	OrderLocalState() {
	}

public:
	//! The local sort state
	LocalSortState local_sort_state;
	//! Local copy of the sorting expression executor
	ExpressionExecutor executor;
	//! Holds a vector of incoming sorting columns
	DataChunk sort;
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
	// Get the payload layout from the return types
	RowLayout payload_layout;
	payload_layout.Initialize(types, false);
	auto state = make_unique<OrderGlobalState>(BufferManager::GetBufferManager(context), *this, payload_layout);
	// Set external (can be force with the PRAGMA)
	state->global_sort_state.external = context.force_external;
	// Memory usage per thread should scale with max mem / num threads
	// We take 1/5th of this, to be conservative
	idx_t max_memory = BufferManager::GetBufferManager(context).GetMaxMemory();
	idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	state->memory_per_thread = (max_memory / num_threads) / 5;
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
	auto result = make_unique<OrderLocalState>();
	// Initialize order clause expression executor and DataChunk
	vector<LogicalType> types;
	for (auto &order : orders) {
		types.push_back(order.expression->return_type);
		result->executor.AddExpression(*order.expression);
	}
	result->sort.Initialize(types);
	return move(result);
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p,
                         DataChunk &input) const {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;

	auto &global_sort_state = gstate.global_sort_state;
	auto &local_sort_state = lstate.local_sort_state;

	// Initialize local state (if necessary)
	if (!local_sort_state.initialized) {
		local_sort_state.Initialize(global_sort_state, BufferManager::GetBufferManager(context.client));
	}

	// Obtain sorting columns
	auto &sort = lstate.sort;
	lstate.executor.Execute(input, sort);

	// Sink the data into the local sort state
	local_sort_state.SinkChunk(sort, input);

	// When sorting data reaches a certain size, we sort it
	if (local_sort_state.SizeInBytes() >= gstate.memory_per_thread) {
		local_sort_state.Sort(global_sort_state);
	}
}

void PhysicalOrder::Combine(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p) {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;
	gstate.global_sort_state.AddLocalState(lstate.local_sort_state);
}

class PhysicalOrderMergeTask : public Task {
public:
	PhysicalOrderMergeTask(Pipeline &parent, ClientContext &context, OrderGlobalState &state)
	    : parent(parent.shared_from_this()), context(context), state(state) {
	}

	void Execute() override {
		// Initialize merge sorted and iterate until done
		auto &global_sort_state = state.global_sort_state;
		MergeSorter merge_sorter(global_sort_state, BufferManager::GetBufferManager(context));
		merge_sorter.PerformInMergeRound();
		// Finish task and act if all tasks are finished
		idx_t finished_tasks = ++parent->finished_tasks;
		if (finished_tasks == parent->total_tasks) {
			global_sort_state.CompleteMergeRound();
			if (global_sort_state.sorted_blocks.size() == 1) {
				// Only one block left: Done!
				parent->Finish();
			} else {
				// Schedule the next round
				PhysicalOrder::ScheduleMergeTasks(*parent, context, state);
			}
		}
	}

private:
	shared_ptr<Pipeline> parent;
	ClientContext &context;
	OrderGlobalState &state;
};

bool PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state_p) {
	this->sink_state = move(state_p);
	auto &state = (OrderGlobalState &)*this->sink_state;
	auto &global_sort_state = state.global_sort_state;

	if (global_sort_state.sorted_blocks.empty()) {
		// Empty input!
		return true;
	}

	// Prepare for merge sort phase
	global_sort_state.PrepareMergePhase();

	// Start the merge phase or finish if a merge is not necessary
	if (global_sort_state.sorted_blocks.size() > 1) {
		PhysicalOrder::ScheduleMergeTasks(pipeline, context, state);
		return false;
	} else {
		return true;
	}
}

void PhysicalOrder::ScheduleMergeTasks(Pipeline &pipeline, ClientContext &context, OrderGlobalState &state) {
	// Initialize global sort state for a round of merging
	state.global_sort_state.InitializeMergeRound();
	// Schedule tasks equal to the number of threads, which will each merge multiple partitions
	auto &ts = TaskScheduler::GetScheduler(context);
	idx_t num_threads = ts.NumberOfThreads();
	pipeline.total_tasks += num_threads;
	for (idx_t tnum = 0; tnum < num_threads; tnum++) {
		auto new_task = make_unique<PhysicalOrderMergeTask>(pipeline, context, state);
		ts.ScheduleTask(pipeline.token, move(new_task));
	}
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderOperatorState(PhysicalOperator &op, PhysicalOperator *child) : PhysicalOperatorState(op, child) {
	}

public:
	//! Payload scanner
	unique_ptr<SortedDataScanner> scanner = nullptr;
};

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(*this, children[0].get());
}

void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                     PhysicalOperatorState *state_p) const {
	auto &state = *reinterpret_cast<PhysicalOrderOperatorState *>(state_p);

	if (!state.scanner) {
		// Initialize scanner (if not yet initialized)
		auto &gstate = (OrderGlobalState &)*this->sink_state;
		auto &global_sort_state = gstate.global_sort_state;
		if (global_sort_state.sorted_blocks.empty()) {
			return;
		}
		state.scanner =
		    make_unique<SortedDataScanner>(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state);
	}

	// Scan the next data chunk
	state.scanner->Scan(chunk);
}

string PhysicalOrder::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += orders[i].expression->ToString() + " ";
		result += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	return result;
}

} // namespace duckdb
