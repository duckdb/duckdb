#include "duckdb/execution/operator/order/physical_order.hpp"

//#include "duckdb/common/assert.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/data_table.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/row_chunk.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders)
    : PhysicalSink(PhysicalOperatorType::ORDER_BY, move(types)), orders(move(orders)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class OrderGlobalState : public GlobalOperatorState {
public:
	OrderGlobalState(BufferManager &buffer_manager) : row_chunk(buffer_manager) {
	}
	// TODO: old
	//! The lock for updating the global aggregate state
	mutex lock;

	// TODO: new
	//! Sorting columns that were computed from PhysicalOrder::Orders
	ExpressionExecutor executor;
	vector<LogicalType> sort_types;
	vector<OrderType> order_types;
	vector<OrderByNullType> null_order_types;

	//! Data in row format
	RowChunk row_chunk;
};

class OrderLocalState : public LocalSinkState {
public:
	OrderLocalState() {
	}
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
	auto state = make_unique<OrderGlobalState>(BufferManager::GetBufferManager(context));
	// compute the sorting columns from the input data
	for (idx_t i = 0; i < orders.size(); i++) {
		auto &expr = orders[i].expression;
		state->sort_types.push_back(expr->return_type);
		state->order_types.push_back(orders[i].type);
		state->null_order_types.push_back(orders[i].null_order);
		state->executor.AddExpression(*expr);
	}
    // compute entry size
	for (auto type : children[0]->types) {
		state->row_chunk.types.push_back(type);
		state->row_chunk.entry_size += GetTypeIdSize(type.InternalType());
	}
	state->row_chunk.block_capacity =
	    MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / state->row_chunk.entry_size) + 1);

	return state;
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<OrderLocalState>();
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                         DataChunk &input) {
	auto &gstate = (OrderGlobalState &)state;

	// get the columns that we sort on
	DataChunk sort_chunk;
	sort_chunk.Initialize(gstate.sort_types);
	gstate.executor.Execute(input, sort_chunk);

	// TODO: think about how we can prevent locking
	lock_guard<mutex> glock(gstate.lock);

	// convert columns to row-wise format
	const SelectionVector *sel_ptr = &FlatVector::IncrementalSelectionVector;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	// all columns
	gstate.row_chunk.Build(input.size(), key_locations);
	for (auto &v : input.data) {
		gstate.row_chunk.SerializeVector(v, input.size(), *sel_ptr, input.size(), key_locations);
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class PhysicalOrderSortTask : public Task {
public:
	PhysicalOrderSortTask(Pipeline &parent_, OrderGlobalState &state_, idx_t block_idx_)
	    : parent(parent_), state(state_), block_idx(block_idx_) {
		entry_size = state.row_chunk.entry_size;
		block = state.row_chunk.blocks[block_idx];
	}

	void Execute() {
		// get data from the buffer manager
//		auto handle = state.row_chunk.buffer_manager.Pin(block.block);
//		auto dataptr = handle->node->buffer;

		// sort the pointers pointing into the block of rows partition
//		Sort(dataptr);

		// re-order data
//		ReOrder(dataptr);

		// give data back to BufferManager
//		state.row_chunk.buffer_manager.Unpin(block.block);

		// finish task
		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
	}

private:
	void Sort(data_ptr_t &dataptr) {
		// TODO:
//		idx_t entry_size = entry_size;
//		std::sort(dataptr, dataptr + count * entry_size, [&entry_size](const_data_ptr_t &lhs, const_data_ptr_t &rhs) {
//			return memcmp((data_ptr_t *)lhs, (data_ptr_t *)rhs, entry_size) < 0;
//		});
	}

	void ReOrder(data_ptr_t &dataptr) {
        // TODO:
	}

	Pipeline &parent;
	OrderGlobalState &state;

	idx_t block_idx;
	idx_t count;

	RowDataBlock block;
	idx_t entry_size;
};

class PhysicalOrderMergeTask : public Task {
public:
	PhysicalOrderMergeTask(Pipeline &parent_, OrderGlobalState &state_) : parent(parent_), state(state_) {
	}

	void Execute() {
		// TODO: merge the things
		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
		// finish the whole pipeline
		if (parent.total_tasks == parent.finished_tasks) {
			parent.Finish();
		}
	}

private:
	Pipeline &parent;
	OrderGlobalState &state;
};

void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	// finalize: perform the actual sorting
	auto &sink = (OrderGlobalState &)*state;

	// schedule sorting tasks for each block
	for (idx_t block_idx = 0; block_idx < sink.row_chunk.blocks.size(); block_idx++) {
		// TODO: sort_cols and row_chunk have different block indices
		auto new_task = make_unique<PhysicalOrderSortTask>(pipeline, sink, block_idx);
		TaskScheduler::GetScheduler(context).ScheduleTask(pipeline.token, move(new_task));
	}

	// FIXME: schedule ?? merging tasks

	// TODO: deserialize

	PhysicalSink::Finalize(pipeline, context, move(state));
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderOperatorState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), position(0) {
	}

	idx_t current_block;
	idx_t position;
};

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(*this, children[0].get());
}

void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_);
	auto &sink = (OrderGlobalState &)*this->sink_state;

	if (state->current_block >= sink.row_chunk.blocks.size()) {
		return;
	}

	sink.row_chunk.DeserializeRowBlock(chunk, sink.row_chunk.blocks[state->current_block], state->position);

	state->position += STANDARD_VECTOR_SIZE;
	if (state->position >= sink.row_chunk.blocks[state->current_block].count) {
		state->current_block++;
	}
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
