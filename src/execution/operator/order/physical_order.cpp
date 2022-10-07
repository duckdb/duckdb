#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, vector<idx_t> projections,
                             idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::ORDER_BY, move(types), estimated_cardinality), orders(move(orders)),
      projections(move(projections)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class OrderGlobalState : public GlobalSinkState {
public:
	OrderGlobalState(BufferManager &buffer_manager, const PhysicalOrder &order, RowLayout &payload_layout)
	    : global_sort_state(buffer_manager, order.orders, payload_layout) {
	}

	//! Global sort state
	GlobalSortState global_sort_state;
	//! Memory usage per thread
	idx_t memory_per_thread;
};

class OrderLocalState : public LocalSinkState {
public:
	OrderLocalState(Allocator &allocator, const PhysicalOrder &op) : key_executor(allocator) {
		// Initialize order clause expression executor and DataChunk
		vector<LogicalType> key_types;
		for (auto &order : op.orders) {
			key_types.push_back(order.expression->return_type);
			key_executor.AddExpression(*order.expression);
		}
		keys.Initialize(allocator, key_types);
		payload.Initialize(allocator, op.types);
	}

public:
	//! The local sort state
	LocalSortState local_sort_state;
	//! Key expression executor, and chunk to hold the vectors
	ExpressionExecutor key_executor;
	DataChunk keys;
	//! Payload chunk to hold the vectors
	DataChunk payload;
};

unique_ptr<GlobalSinkState> PhysicalOrder::GetGlobalSinkState(ClientContext &context) const {
	// Get the payload layout from the return types
	RowLayout payload_layout;
	payload_layout.Initialize(types);
	auto state = make_unique<OrderGlobalState>(BufferManager::GetBufferManager(context), *this, payload_layout);
	// Set external (can be force with the PRAGMA)
	state->global_sort_state.external = ClientConfig::GetConfig(context).force_external;
	state->memory_per_thread = GetMaxThreadMemory(context);
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<OrderLocalState>(Allocator::Get(context.client), *this);
}

SinkResultType PhysicalOrder::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
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
	auto &keys = lstate.keys;
	keys.Reset();
	lstate.key_executor.Execute(input, keys);

	auto &payload = lstate.payload;
	payload.ReferenceColumns(input, projections);

	// Sink the data into the local sort state
	keys.Verify();
	input.Verify();
	local_sort_state.SinkChunk(keys, payload);

	// When sorting data reaches a certain size, we sort it
	if (local_sort_state.SizeInBytes() >= gstate.memory_per_thread) {
		local_sort_state.Sort(global_sort_state, true);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalOrder::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;
	gstate.global_sort_state.AddLocalState(lstate.local_sort_state);
}

class PhysicalOrderMergeTask : public ExecutorTask {
public:
	PhysicalOrderMergeTask(shared_ptr<Event> event_p, ClientContext &context, OrderGlobalState &state)
	    : ExecutorTask(context), event(move(event_p)), context(context), state(state) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		// Initialize merge sorted and iterate until done
		auto &global_sort_state = state.global_sort_state;
		MergeSorter merge_sorter(global_sort_state, BufferManager::GetBufferManager(context));
		merge_sorter.PerformInMergeRound();
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	ClientContext &context;
	OrderGlobalState &state;
};

class OrderMergeEvent : public BasePipelineEvent {
public:
	OrderMergeEvent(OrderGlobalState &gstate_p, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p) {
	}

	OrderGlobalState &gstate;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		// Schedule tasks equal to the number of threads, which will each merge multiple partitions
		auto &ts = TaskScheduler::GetScheduler(context);
		idx_t num_threads = ts.NumberOfThreads();

		vector<unique_ptr<Task>> merge_tasks;
		for (idx_t tnum = 0; tnum < num_threads; tnum++) {
			merge_tasks.push_back(make_unique<PhysicalOrderMergeTask>(shared_from_this(), context, gstate));
		}
		SetTasks(move(merge_tasks));
	}

	void FinishEvent() override {
		auto &global_sort_state = gstate.global_sort_state;

		global_sort_state.CompleteMergeRound();
		if (global_sort_state.sorted_blocks.size() > 1) {
			// Multiple blocks remaining: Schedule the next round
			PhysicalOrder::ScheduleMergeTasks(*pipeline, *this, gstate);
		}
	}
};

SinkFinalizeType PhysicalOrder::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         GlobalSinkState &gstate_p) const {
	auto &state = (OrderGlobalState &)gstate_p;
	auto &global_sort_state = state.global_sort_state;

	if (global_sort_state.sorted_blocks.empty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Prepare for merge sort phase
	global_sort_state.PrepareMergePhase();

	// Start the merge phase or finish if a merge is not necessary
	if (global_sort_state.sorted_blocks.size() > 1) {
		PhysicalOrder::ScheduleMergeTasks(pipeline, event, state);
	}
	return SinkFinalizeType::READY;
}

void PhysicalOrder::ScheduleMergeTasks(Pipeline &pipeline, Event &event, OrderGlobalState &state) {
	// Initialize global sort state for a round of merging
	state.global_sort_state.InitializeMergeRound();
	auto new_event = make_shared<OrderMergeEvent>(state, pipeline);
	event.InsertEvent(move(new_event));
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PhysicalOrderOperatorState : public GlobalSourceState {
public:
	//! Payload scanner
	unique_ptr<PayloadScanner> scanner;
};

unique_ptr<GlobalSourceState> PhysicalOrder::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PhysicalOrderOperatorState>();
}

void PhysicalOrder::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                            LocalSourceState &lstate) const {
	auto &state = (PhysicalOrderOperatorState &)gstate;

	if (!state.scanner) {
		// Initialize scanner (if not yet initialized)
		auto &gstate = (OrderGlobalState &)*this->sink_state;
		auto &global_sort_state = gstate.global_sort_state;
		if (global_sort_state.sorted_blocks.empty()) {
			return;
		}
		state.scanner =
		    make_unique<PayloadScanner>(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state);
	}

	// Scan the next data chunk
	state.scanner->Scan(chunk);
}

string PhysicalOrder::ParamsToString() const {
	string result = "ORDERS:\n";
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
