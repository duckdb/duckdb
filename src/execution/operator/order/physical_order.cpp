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
	OrderGlobalState(PhysicalOrder &_op, BufferManager &buffer_manager) : op(_op), sorting(buffer_manager), payload(buffer_manager) {
	}
	// TODO: old
	//! The lock for updating the global aggregate state
	mutex lock;

	// TODO: new
	PhysicalOrder &op;

	//! Sorting columns in row format
	RowChunk sorting;
	//! Sizes of the sorting columns
	vector<idx_t> sorting_sizes;
    //! To execute the expressions that are sorted
    ExpressionExecutor executor;
	//! Payload in row format
	RowChunk payload;
};

class OrderLocalState : public LocalSinkState {
public:
	OrderLocalState() {
	}
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
	auto state = make_unique<OrderGlobalState>(*this, BufferManager::GetBufferManager(context));

	// sorting columns
	auto &sorting = state->sorting;
	sorting.nullmask_size = (orders.size() + 7) / 8;
	sorting.entry_size += sorting.nullmask_size;
    for (idx_t i = 0; i < orders.size(); i++) {
        state->sorting_sizes.push_back(GetTypeIdSize(orders[i].expression->return_type.InternalType()));
		sorting.entry_size += GetTypeIdSize(orders[i].expression->return_type.InternalType());
        state->executor.AddExpression(*orders[i].expression);
	}
    sorting.block_capacity =
        MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / sorting.entry_size) + 1);

	// payload columns
	auto &payload = state->payload;
	payload.nullmask_size = (types.size() + 7) / 8;
	payload.entry_size += payload.nullmask_size;
    for (auto type : types) {
        payload.entry_size += GetTypeIdSize(type.InternalType());
    }
    payload.block_capacity =
        MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / payload.entry_size) + 1);

	return state;
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<OrderLocalState>();
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                         DataChunk &input) {
	auto &gstate = (OrderGlobalState &)state;

	// execute expressions that are sorted
	vector<LogicalType> sort_types;
	for (auto &order : orders) {
        sort_types.push_back(order.expression->return_type);
	}
    DataChunk sort;
	sort.Initialize(sort_types);
    gstate.executor.Execute(input, sort);

	// TODO: think about how we can prevent locking
	lock_guard<mutex> glock(gstate.lock);

	// initialize pointers
	const SelectionVector *sel_ptr = &FlatVector::IncrementalSelectionVector;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t nullmask_locations[STANDARD_VECTOR_SIZE];

	// sorting columns
	gstate.sorting.Build(sort.size(), key_locations, nullmask_locations);
    for (idx_t i = 0; i < sort.data.size(); i++) {
        gstate.sorting.SerializeVector(sort.data[i], sort.size(), *sel_ptr, sort.size(), i, key_locations,
                                       nullmask_locations);
    }

	// payload columns
	gstate.payload.Build(input.size(), key_locations, nullmask_locations);
	for (idx_t i = 0; i < input.data.size(); i++) {
		gstate.payload.SerializeVector(input.data[i], input.size(), *sel_ptr, input.size(), i, key_locations,
		                                 nullmask_locations);
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class PhysicalOrderSortTask : public Task {
public:
	PhysicalOrderSortTask(Pipeline &parent_, OrderGlobalState &state_, idx_t block_idx_)
	    : parent(parent_), state(state_), buffer_manager(state_.payload.buffer_manager), block_idx(block_idx_),
	      sort(state.sorting.blocks[block_idx]), payl(state.payload.blocks[block_idx]) {
	}

	void Execute() {
		// get sorting data from the buffer manager
		auto handle = buffer_manager.Pin(sort.block);
		auto dataptr = handle->node->buffer;

		// fetch a batch of pointers to entries in the blocks
		data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
		auto idxs = unique_ptr<idx_t[]>(new idx_t[sort.count]);
		for (idx_t i = 0; i < sort.count; i++) {
			key_locations[i] = dataptr;
			idxs[i] = i;
			dataptr += state.sorting.entry_size;
		}

		// sort the indices
		Sort(key_locations, idxs.get());

		// re-order sorting data
		ReOrder(state.sorting, key_locations, idxs.get());

		// now re-order payload data
		handle = buffer_manager.Pin(payl.block);
		dataptr = handle->node->buffer;
        for (idx_t i = 0; i < payl.count; i++) {
            key_locations[i] = dataptr;
            dataptr += state.payload.entry_size;
        }
		ReOrder(state.payload, key_locations, idxs.get());

		// finish task
		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
	}

private:
	void Sort(data_ptr_t key_locations[], idx_t idxs[]) {
		// create reference so it can be used in the lambda function
		auto *state_ptr = &state;
		std::sort(idxs, idxs + sort.count,
		          [key_locations, state_ptr](const idx_t &l_i, const idx_t &r_i) { return CompareTuple(l_i, r_i, key_locations, state_ptr) <= 0; });
	}

	static int CompareTuple(const idx_t &l_i, const idx_t &r_i, data_ptr_t key_locations[], OrderGlobalState *state) {
        data_ptr_t l_nullmask = key_locations[l_i];
        data_ptr_t r_nullmask = key_locations[r_i];
		data_ptr_t lhs = key_locations[l_i] + state->sorting.nullmask_size;
        data_ptr_t rhs = key_locations[r_i] + state->sorting.nullmask_size;
		for (idx_t i = 0; i < state->op.orders.size(); i++) {
			auto comp_res = CompareValue(l_nullmask, r_nullmask, lhs, rhs, i, state);
			if (comp_res == 0) {
                lhs += state->sorting_sizes[i];
                rhs += state->sorting_sizes[i];
				continue;
			}
			return comp_res < 0 ? (state->op.orders[i].type == OrderType::ASCENDING ? -1 : 1)
			                    : (state->op.orders[i].type == OrderType::ASCENDING ? 1 : -1);
		}
		return 0;
	}

	static int32_t CompareValue(data_ptr_t &l_nullmask, data_ptr_t &r_nullmask, data_ptr_t &lhs, data_ptr_t &rhs, const idx_t &sort_idx, OrderGlobalState *state) {
		bool left_null = *l_nullmask & (1 << sort_idx);
		bool right_null = *r_nullmask & (1 << sort_idx);

		if (left_null && right_null) {
			return 0;
		} else if (right_null) {
			return state->op.orders[sort_idx].null_order == OrderByNullType::NULLS_FIRST ? 1 : -1;
		} else if (left_null) {
			return state->op.orders[sort_idx].null_order == OrderByNullType::NULLS_FIRST ? -1 : 1;
		}

		return memcmp(lhs, rhs, state->sorting_sizes[sort_idx]);
	}

	void ReOrder(RowChunk &chunk, data_ptr_t key_locations[], idx_t idxs[]) {
        // reference to old block
        auto &old_block = chunk.blocks[block_idx];

        // initialize new block with same size as old block
        RowDataBlock new_block;
        new_block.count = old_block.count;
        new_block.capacity = old_block.capacity;
        new_block.block = buffer_manager.RegisterMemory(chunk.block_capacity * chunk.entry_size, false);

        // pin the new block
        auto new_buffer_handle = buffer_manager.Pin(new_block.block);
        auto dataptr = new_buffer_handle->node->buffer;

        // copy data in correct order and unpin the new block when done
        for (idx_t i = 0; i < old_block.count; i++) {
            memcpy(dataptr, key_locations[idxs[i]], chunk.entry_size);
            dataptr += chunk.entry_size;
        }

        // replace old block with new block, and unregister the old block
        chunk.blocks[block_idx] = new_block;
        buffer_manager.UnregisterBlock(old_block.block->BlockId(), true);
	}

	Pipeline &parent;
	OrderGlobalState &state;
	BufferManager &buffer_manager;

	idx_t block_idx;
	RowDataBlock sort;
    RowDataBlock payl;
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
	for (idx_t i = 0; i < sink.payload.blocks.size(); i++) {
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

	if (state->current_block >= sink.payload.blocks.size()) {
		return;
	}

	sink.payload.DeserializeRowBlock(chunk, sink.payload.blocks[state->current_block], state->position);

	state->position += STANDARD_VECTOR_SIZE;
	if (state->position >= sink.payload.blocks[state->current_block].count) {
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
