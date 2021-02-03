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
	OrderGlobalState(PhysicalOrder &_op, BufferManager &buffer_manager) : op(_op), row_chunk(buffer_manager) {
	}
	// TODO: old
	//! The lock for updating the global aggregate state
	mutex lock;

	// TODO: new
	PhysicalOrder &op;
	//! Data in row format
	RowChunk row_chunk;

	vector<idx_t> sort_indices;
	vector<idx_t> sort_offsets;
};

class OrderLocalState : public LocalSinkState {
public:
	OrderLocalState() {
	}
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
	auto state = make_unique<OrderGlobalState>(*this, BufferManager::GetBufferManager(context));
	auto &row_chunk = state->row_chunk;

	// nullmask bitset, one bit for each column
	row_chunk.nullmask_size = (children[0]->types.size() + 7) / 8;
	row_chunk.entry_size += row_chunk.nullmask_size;

	// size of each column and total block capacity
	vector<idx_t> column_offsets;
	for (auto type : children[0]->types) {
		column_offsets.push_back(row_chunk.entry_size);
		row_chunk.entry_size += GetTypeIdSize(type.InternalType());
	}
	row_chunk.block_capacity =
	    MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / row_chunk.entry_size) + 1);

	// store offsets of the columns that are sorted
	for (idx_t i = 0; i < orders.size(); i++) {
		auto &ref = (BoundReferenceExpression &)*orders[i].expression;
		state->sort_indices.push_back(ref.index);
		state->sort_offsets.push_back(column_offsets[ref.index]);
	}

	return state;
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<OrderLocalState>();
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                         DataChunk &input) {
	auto &gstate = (OrderGlobalState &)state;

	// TODO: think about how we can prevent locking
	lock_guard<mutex> glock(gstate.lock);

	// initialize pointers
	const SelectionVector *sel_ptr = &FlatVector::IncrementalSelectionVector;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t nullmask_locations[STANDARD_VECTOR_SIZE];
	// now convert to row-wise format
	gstate.row_chunk.Build(input.size(), key_locations, nullmask_locations);
	for (idx_t i = 0; i < input.data.size(); i++) {
		gstate.row_chunk.SerializeVector(input.data[i], input.size(), *sel_ptr, input.size(), i, key_locations,
		                                 nullmask_locations);
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class PhysicalOrderSortTask : public Task {
public:
	PhysicalOrderSortTask(Pipeline &parent_, OrderGlobalState &state_, idx_t block_idx_)
	    : parent(parent_), state(state_), buffer_manager(state_.row_chunk.buffer_manager), block_idx(block_idx_),
	      block(state.row_chunk.blocks[block_idx]) {
	}

	void Execute() {
		// get data from the buffer manager
		auto handle = buffer_manager.Pin(block.block);
		auto dataptr = handle->node->buffer;

		// fetch a batch of pointers to entries in the blocks
		data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
		for (idx_t i = 0; i < block.count; i++) {
			key_locations[i] = dataptr;
			dataptr += state.row_chunk.entry_size;
		}
		// sort the pointers
		Sort(key_locations);

		// re-order data
		ReOrder(key_locations);

		// finish task
		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
	}

private:
	void Sort(data_ptr_t key_locations[]) {
		// create reference so it can be used in the lambda function
		auto *state_ref = &state;
		std::sort(key_locations, key_locations + block.count,
		          [state_ref](data_ptr_t &lhs, data_ptr_t &rhs) { return CompareTuple(lhs, rhs, state_ref) <= 0; });
	}

	static int CompareTuple(data_ptr_t &lhs, data_ptr_t &rhs, OrderGlobalState *state) {
		for (idx_t i = 0; i < state->op.orders.size(); i++) {
			auto comp_res = CompareValue(lhs, rhs, i, state);
			if (comp_res == 0) {
				continue;
			}
			return comp_res < 0 ? (state->op.orders[i].type == OrderType::ASCENDING ? -1 : 1)
			                    : (state->op.orders[i].type == OrderType::ASCENDING ? 1 : -1);
		}
		return 0;
	}

	static int32_t CompareValue(data_ptr_t &lhs, data_ptr_t &rhs, const idx_t &sort_idx, OrderGlobalState *state) {
		bool left_null = *lhs & (1 << state->sort_indices[sort_idx]);
		bool right_null = *rhs & (1 << state->sort_indices[sort_idx]);

		if (left_null && right_null) {
			return 0;
		} else if (right_null) {
			return state->op.orders[sort_idx].null_order == OrderByNullType::NULLS_FIRST ? 1 : -1;
		} else if (left_null) {
			return state->op.orders[sort_idx].null_order == OrderByNullType::NULLS_FIRST ? -1 : 1;
		}

		return memcmp(lhs + state->sort_offsets[sort_idx], rhs + state->sort_offsets[sort_idx],
		              GetTypeIdSize(state->op.orders[sort_idx].expression->return_type.InternalType()));
	}

	void ReOrder(data_ptr_t key_locations[]) {
		auto ordered_block =
		    buffer_manager.RegisterMemory(state.row_chunk.block_capacity * state.row_chunk.entry_size, false);
		auto handle = buffer_manager.Pin(ordered_block);

		RowDataBlock new_block;
		new_block.count = block.count;
		new_block.capacity = block.capacity;
		new_block.block = move(ordered_block);

		// copy data in correct order
		auto dataptr = handle->node->buffer;
		for (idx_t i = 0; i < block.count; i++) {
			memcpy(dataptr, key_locations[i], state.row_chunk.entry_size);
			dataptr += state.row_chunk.entry_size;
		}

		// unregister unsorted data, and replace the block
		buffer_manager.UnregisterBlock(block.block->BlockId(), true);
		state.row_chunk.blocks[block_idx] = new_block;
	}

	Pipeline &parent;
	OrderGlobalState &state;
	BufferManager &buffer_manager;

	idx_t block_idx;
	RowDataBlock block;
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
	for (idx_t i = 0; i < sink.row_chunk.blocks.size(); i++) {
		// TODO: sort_cols and row_chunk have different block indices
		auto new_task = make_unique<PhysicalOrderSortTask>(pipeline, sink, i);
		TaskScheduler::GetScheduler(context).ScheduleTask(pipeline.token, move(new_task));
	}

	// FIXME: schedule ?? merging tasks

	PhysicalSink::Finalize(pipeline, context, move(state));
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderOperatorState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), current_block(0), position(0) {
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
