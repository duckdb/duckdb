#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/assert.hpp"
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
    state->sort_cols.block_capacity = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / state->sort_cols.entry_size) + 1);

	for (auto type : children[0]->types) {
		state->all_cols.entry_size += GetTypeIdSize(type.InternalType());
	}
	state->all_cols.block_capacity = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_ALLOC_SIZE / state->all_cols.entry_size) + 1);

	return state;
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
    return make_unique<OrderLocalState>();
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                         DataChunk &input) {
	// concatenate all the data of the child chunks
	auto &gstate = (OrderGlobalState &)state;

    // convert columns to be sorted to row-wise format
    SelectionVector sel(STANDARD_VECTOR_SIZE);
    const SelectionVector *sel_ptr = &sel;

    data_ptr_t key_locations[STANDARD_VECTOR_SIZE];

	DataChunk sort_chunk;
	gstate.executor.Execute(input, sort_chunk);

    // TODO: think about how we can prevent locking
    lock_guard<mutex> glock(gstate.lock);

	// serialize the columns that we sort on
    gstate.sort_cols.Build(input.size(), key_locations);
	for (auto &v : sort_chunk.data) {
		gstate.sort_cols.SerializeVector(v, input.size(), *sel_ptr, input.size(), key_locations);
	}
	// serialize all columns
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
    PhysicalOrderSortTask(Pipeline &parent_, OrderGlobalState &state_, idx_t block_idx_) : parent(parent_), state(state_), block_idx(block_idx_) {
	}

	void Execute() {
        auto sort_entry_size = state.sort_cols.entry_size;
		auto sort_block = state.sort_cols.blocks[block_idx];
        auto sort_handle = state.sort_cols.buffer_manager.Pin(sort_block.block);
        data_ptr_t sort_dataptr = sort_handle->node->buffer;

		// store indices
		auto sorted_indices = unique_ptr<idx_t[]>(new idx_t[sort_block.count]);
		for (idx_t i = 0; i < sort_block.count; i++) {
			sorted_indices[i] = i;
		}

		// sort the indices
        std::sort(sorted_indices.get(), sorted_indices.get() + sort_block.count, [&sort_dataptr, &sort_entry_size](const idx_t &lhs, const idx_t &rhs) {
            return memcmp(sort_dataptr + lhs * sort_entry_size, sort_dataptr + rhs * sort_entry_size, sort_entry_size) < 0;
        });

        auto all_entry_size = state.all_cols.entry_size;
        auto all_block = state.all_cols.blocks[block_idx];
        auto all_handle = state.all_cols.buffer_manager.Pin(sort_block.block);
        data_ptr_t all_dataptr = all_handle->node->buffer;

        // use the indices to re-order the data
		for (idx_t i = 0; i < sort_block.count; i++) {
			std::swap_ranges(sort_dataptr + i * sort_entry_size, sort_dataptr + sorted_indices[i] * sort_entry_size,
			                 sort_entry_size);
            std::swap_ranges(all_dataptr + i * all_entry_size, all_dataptr + sorted_indices[i] * all_entry_size,
                             all_entry_size);
		}

		// TODO: give data back to BufferManager

        lock_guard<mutex> glock(state.lock);
        parent.finished_tasks++;
	}

private:
    Pipeline &parent;
    OrderGlobalState &state;
	idx_t block_idx;
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
