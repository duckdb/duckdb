#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, vector<idx_t> projections,
                             idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::ORDER_BY, std::move(types), estimated_cardinality),
      orders(std::move(orders)), projections(std::move(projections)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class OrderGlobalSinkState : public GlobalSinkState {
public:
	OrderGlobalSinkState(BufferManager &buffer_manager, const PhysicalOrder &order, RowLayout &payload_layout)
	    : order(order), global_sort_state(buffer_manager, order.orders, payload_layout) {
	}

	const PhysicalOrder &order;
	//! Global sort state
	GlobalSortState global_sort_state;
	//! Memory usage per thread
	idx_t memory_per_thread;
};

class OrderLocalSinkState : public LocalSinkState {
public:
	OrderLocalSinkState(ClientContext &context, const PhysicalOrder &op) : key_executor(context) {
		// Initialize order clause expression executor and DataChunk
		vector<LogicalType> key_types;
		for (auto &order : op.orders) {
			key_types.push_back(order.expression->return_type);
			key_executor.AddExpression(*order.expression);
		}
		auto &allocator = Allocator::Get(context);
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
	auto state = make_uniq<OrderGlobalSinkState>(BufferManager::GetBufferManager(context), *this, payload_layout);
	// Set external (can be force with the PRAGMA)
	state->global_sort_state.external = ClientConfig::GetConfig(context).force_external;
	state->memory_per_thread = GetMaxThreadMemory(context);
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<OrderLocalSinkState>(context.client, *this);
}

SinkResultType PhysicalOrder::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<OrderGlobalSinkState>();
	auto &lstate = input.local_state.Cast<OrderLocalSinkState>();

	auto &global_sort_state = gstate.global_sort_state;
	auto &local_sort_state = lstate.local_sort_state;

	// Initialize local state (if necessary)
	if (!local_sort_state.initialized) {
		local_sort_state.Initialize(global_sort_state, BufferManager::GetBufferManager(context.client));
	}

	// Obtain sorting columns
	auto &keys = lstate.keys;
	keys.Reset();
	lstate.key_executor.Execute(chunk, keys);

	auto &payload = lstate.payload;
	payload.ReferenceColumns(chunk, projections);

	// Sink the data into the local sort state
	keys.Verify();
	chunk.Verify();
	local_sort_state.SinkChunk(keys, payload);

	// When sorting data reaches a certain size, we sort it
	if (local_sort_state.SizeInBytes() >= gstate.memory_per_thread) {
		local_sort_state.Sort(global_sort_state, true);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalOrder::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<OrderGlobalSinkState>();
	auto &lstate = input.local_state.Cast<OrderLocalSinkState>();
	gstate.global_sort_state.AddLocalState(lstate.local_sort_state);

	return SinkCombineResultType::FINISHED;
}

class PhysicalOrderMergeTask : public ExecutorTask {
public:
	PhysicalOrderMergeTask(shared_ptr<Event> event_p, ClientContext &context, OrderGlobalSinkState &state,
	                       const PhysicalOperator &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), context(context), state(state) {
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
	ClientContext &context;
	OrderGlobalSinkState &state;
};

class OrderMergeEvent : public BasePipelineEvent {
public:
	OrderMergeEvent(OrderGlobalSinkState &gstate_p, Pipeline &pipeline_p, const PhysicalOperator &op_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p), op(op_p) {
	}

	OrderGlobalSinkState &gstate;
	const PhysicalOperator &op;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		// Schedule tasks equal to the number of threads, which will each merge multiple partitions
		auto &ts = TaskScheduler::GetScheduler(context);
		auto num_threads = NumericCast<idx_t>(ts.NumberOfThreads());

		vector<shared_ptr<Task>> merge_tasks;
		for (idx_t tnum = 0; tnum < num_threads; tnum++) {
			merge_tasks.push_back(make_uniq<PhysicalOrderMergeTask>(shared_from_this(), context, gstate, op));
		}
		SetTasks(std::move(merge_tasks));
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
                                         OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<OrderGlobalSinkState>();
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

void PhysicalOrder::ScheduleMergeTasks(Pipeline &pipeline, Event &event, OrderGlobalSinkState &state) {
	// Initialize global sort state for a round of merging
	state.global_sort_state.InitializeMergeRound();
	auto new_event = make_shared_ptr<OrderMergeEvent>(state, pipeline, state.order);
	event.InsertEvent(std::move(new_event));
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PhysicalOrderGlobalSourceState : public GlobalSourceState {
public:
	explicit PhysicalOrderGlobalSourceState(OrderGlobalSinkState &sink) : next_batch_index(0) {
		auto &global_sort_state = sink.global_sort_state;
		if (global_sort_state.sorted_blocks.empty()) {
			total_batches = 0;
		} else {
			D_ASSERT(global_sort_state.sorted_blocks.size() == 1);
			total_batches = global_sort_state.sorted_blocks[0]->payload_data->data_blocks.size();
		}
	}

	idx_t MaxThreads() override {
		return total_batches;
	}

public:
	atomic<idx_t> next_batch_index;
	idx_t total_batches;
};

unique_ptr<GlobalSourceState> PhysicalOrder::GetGlobalSourceState(ClientContext &context) const {
	auto &sink = this->sink_state->Cast<OrderGlobalSinkState>();
	return make_uniq<PhysicalOrderGlobalSourceState>(sink);
}

class PhysicalOrderLocalSourceState : public LocalSourceState {
public:
	explicit PhysicalOrderLocalSourceState(PhysicalOrderGlobalSourceState &gstate)
	    : batch_index(gstate.next_batch_index++) {
	}

public:
	idx_t batch_index;
	unique_ptr<PayloadScanner> scanner;
};

unique_ptr<LocalSourceState> PhysicalOrder::GetLocalSourceState(ExecutionContext &context,
                                                                GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<PhysicalOrderGlobalSourceState>();
	return make_uniq<PhysicalOrderLocalSourceState>(gstate);
}

SourceResultType PhysicalOrder::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<PhysicalOrderGlobalSourceState>();
	auto &lstate = input.local_state.Cast<PhysicalOrderLocalSourceState>();

	if (lstate.scanner && lstate.scanner->Remaining() == 0) {
		lstate.batch_index = gstate.next_batch_index++;
		lstate.scanner = nullptr;
	}

	if (lstate.batch_index >= gstate.total_batches) {
		return SourceResultType::FINISHED;
	}

	if (!lstate.scanner) {
		auto &sink = this->sink_state->Cast<OrderGlobalSinkState>();
		auto &global_sort_state = sink.global_sort_state;
		lstate.scanner = make_uniq<PayloadScanner>(global_sort_state, lstate.batch_index, true);
	}

	lstate.scanner->Scan(chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

idx_t PhysicalOrder::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                   LocalSourceState &lstate_p) const {
	auto &lstate = lstate_p.Cast<PhysicalOrderLocalSourceState>();
	return lstate.batch_index;
}

InsertionOrderPreservingMap<string> PhysicalOrder::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string orders_info;
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			orders_info += "\n";
		}
		orders_info += orders[i].expression->ToString() + " ";
		orders_info += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	result["__order_by__"] = orders_info;
	return result;
}

} // namespace duckdb
