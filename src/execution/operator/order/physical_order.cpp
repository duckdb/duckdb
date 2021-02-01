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
	OrderGlobalState(BufferManager &buffer_manager) : sort_cols(buffer_manager), all_cols(buffer_manager) {
	}
	// TODO: old
	//! The lock for updating the global aggregate state
	mutex lock;
	//! The sorted data
	ChunkCollection sorted_data;
	//! The sorted vector
	unique_ptr<idx_t[]> sorted_vector;

	// TODO: new
	//! Sorting columns that were computed from PhysicalOrder::Orders
	ExpressionExecutor executor;
	vector<LogicalType> sort_types;
	vector<OrderType> order_types;
	vector<OrderByNullType> null_order_types;

	//! Columns to be sorted
	RowChunk sort_cols;
	//! All columns
	RowChunk all_cols;
	//! Min-max of every block
	std::unordered_map<block_id_t, std::pair<unique_ptr<data_ptr_t[]>, unique_ptr<data_ptr_t[]>>> min_max;
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
		state->sort_cols.entry_size += GetTypeIdSize(expr->return_type.InternalType());
	}
	state->sort_cols.block_capacity =
	    MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / state->sort_cols.entry_size) + 1);

	for (auto type : children[0]->types) {
		state->all_cols.entry_size += GetTypeIdSize(type.InternalType());
	}
	state->all_cols.block_capacity =
	    MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / state->all_cols.entry_size) + 1);

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
	// sorting columns
	gstate.sort_cols.Build(input.size(), key_locations);
	for (auto &v : sort_chunk.data) {
		gstate.sort_cols.SerializeVector(v, input.size(), *sel_ptr, input.size(), key_locations);
	}
	// all columns
	gstate.all_cols.Build(input.size(), key_locations);
	for (auto &v : input.data) {
		gstate.all_cols.SerializeVector(v, input.size(), *sel_ptr, input.size(), key_locations);
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class PhysicalOrderSortTask : public Task {
public:
	PhysicalOrderSortTask(Pipeline &parent_, OrderGlobalState &state_, idx_t block_idx_)
	    : parent(parent_), state(state_), block_idx(block_idx_) {
		sort_entry_size = state.sort_cols.entry_size;
		all_entry_size = state.all_cols.entry_size;
		sort_block = state.sort_cols.blocks[block_idx];
		all_block = state.all_cols.blocks[block_idx];
	}

	void Execute() {
		// get data from the buffer manager
		auto sort_handle = state.sort_cols.buffer_manager.Pin(sort_block.block);
		auto all_handle = state.all_cols.buffer_manager.Pin(sort_block.block);
		auto sort_dataptr = sort_handle->node->buffer;
		auto all_dataptr = sort_handle->node->buffer;

		// sort the indices
		auto sorted_indices = unique_ptr<idx_t[]>(new idx_t[count]);
		Sort(sorted_indices.get(), sort_dataptr);

		// re-order data in-place in O(2N)
		ReOrder(sorted_indices.get(), sort_dataptr, all_dataptr);

		// store the min-max of this block
        auto min = unique_ptr<data_ptr_t[]>(new data_ptr_t[sort_entry_size]);
        auto max = unique_ptr<data_ptr_t[]>(new data_ptr_t[sort_entry_size]);
		memcpy(min.get(), sort_dataptr, sort_entry_size);
        memcpy(max.get(), sort_dataptr + (count - 1) * sort_entry_size, sort_entry_size);
		state.min_max[sort_handle->handle->BlockId()] = std::make_pair<unique_ptr<data_ptr_t[]>, unique_ptr<data_ptr_t[]>>(move(min), move(max));

		// give data back to BufferManager
		state.sort_cols.buffer_manager.Unpin(sort_block.block);
		state.all_cols.buffer_manager.Unpin(all_block.block);

		// finish task
		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
	}

private:
	void Sort(idx_t *indices, data_ptr_t &dataptr) {
		// initialize indices array
		for (idx_t i = 0; i < count; i++) {
			indices[i] = i;
		}
		// sort the indices TODO: implement a compare method
		auto &entry_size = sort_entry_size;
		std::sort(indices, indices + count, [&dataptr, &entry_size](const idx_t &lhs, const idx_t &rhs) {
			return memcmp(dataptr + lhs * entry_size, dataptr + rhs * entry_size, entry_size) < 0;
		});
	}

	//! In-place array re-ordering, taken from StackOverflow
	//! https://stackoverflow.com/questions/7365814/in-place-array-reordering
	void ReOrder(idx_t *indices, data_ptr_t &sort_dataptr, data_ptr_t &all_dataptr) {
		// use unique pointers so we can define variable length arrays
		auto sort_temp_ptr = unique_ptr<data_ptr_t[]>(new data_ptr_t[sort_entry_size]);
		auto all_temp_ptr = unique_ptr<data_ptr_t[]>(new data_ptr_t[all_entry_size]);
		// initialize everything outside of the loop
		auto sort_temp = sort_temp_ptr.get();
		auto all_temp = all_temp_ptr.get();
		idx_t j;
		idx_t k;
		for (idx_t i = 0; i < count; i++) {
			memcpy(sort_temp, sort_dataptr + i * sort_entry_size, sort_entry_size);
			memcpy(all_temp, all_dataptr + i * all_entry_size, all_entry_size);
			for (j = i;; j = k) {
				k = indices[j];
				indices[j] = j;
				if (k == i) {
					break;
				}
				memcpy(sort_dataptr + j * sort_entry_size, sort_dataptr + k * sort_entry_size, sort_entry_size);
				memcpy(all_dataptr + j * all_entry_size, all_dataptr + k * all_entry_size, all_entry_size);
			}
			memcpy(sort_dataptr + j * sort_entry_size, sort_temp, sort_entry_size);
			memcpy(all_dataptr + j * all_entry_size, all_temp, all_entry_size);
		}
	}

	Pipeline &parent;
	OrderGlobalState &state;

	idx_t block_idx;
	idx_t count;

	RowDataBlock sort_block;
	idx_t sort_entry_size;

	RowDataBlock all_block;
	idx_t all_entry_size;
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
	for (idx_t block_idx = 0; block_idx < sink.sort_cols.blocks.size(); block_idx++) {
		// TODO: sort_cols and all_cols have different block indices
		auto new_task = make_unique<PhysicalOrderSortTask>(pipeline, sink, block_idx);
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
	    : PhysicalOperatorState(op, child), position(0) {
	}

	idx_t position;
};

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(*this, children[0].get());
}

void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_);
	auto &sink = (OrderGlobalState &)*this->sink_state;
	ChunkCollection &big_data = sink.sorted_data;
	if (state->position >= big_data.Count()) {
		return;
	}

	big_data.MaterializeSortedChunk(chunk, sink.sorted_vector.get(), state->position);
	state->position += STANDARD_VECTOR_SIZE;
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
