#include "duckdb/execution/operator/order/physical_order.hpp"

#include "blockquicksort_wrapper.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t estimated_cardinality)
    : PhysicalSink(PhysicalOperatorType::ORDER_BY, move(types), estimated_cardinality), orders(move(orders)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct SortingState {
	const idx_t ENTRY_SIZE;

	const vector<OrderType> ORDER_TYPES;
	const vector<OrderByNullType> ORDER_BY_NULL_TYPES;
	const vector<LogicalType> TYPES;
	const vector<BaseStatistics *> STATS;
};

struct PayloadState {
	const idx_t NULLMASK_SIZE;
	const idx_t ENTRY_SIZE;
};

class OrderGlobalState : public GlobalOperatorState {
public:
	OrderGlobalState(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
	}
	//! The lock for updating the global order state
	mutex lock;

	BufferManager &buffer_manager;
	unique_ptr<RowChunk> sorting_block;
	unique_ptr<RowChunk> payload_block;

	//! To execute the expressions that are sorted
	ExpressionExecutor executor;

	//! Constants concerning sorting and/or payload data
	unique_ptr<SortingState> sorting_state;
	unique_ptr<PayloadState> payload_state;
};

class OrderLocalState : public LocalSinkState {
public:
	//! Holds a vector of incoming sorting columns
	DataChunk sort;

	unique_ptr<RowChunk> sorting_block = nullptr;
	unique_ptr<RowChunk> payload_block = nullptr;

	//! Used for vector serialization
	const SelectionVector *sel_ptr = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t validitymask_locations[STANDARD_VECTOR_SIZE];
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto state = make_unique<OrderGlobalState>(buffer_manager);

	// init sorting state and sorting block
	size_t entry_size = 0;
	vector<OrderType> order_types;
	vector<OrderByNullType> order_by_null_types;
	vector<LogicalType> types;
	vector<BaseStatistics *> stats;
	for (auto &order : orders) {
		// global state ExpressionExecutor
		auto &expr = *order.expression;
		state->executor.AddExpression(expr);

		// sorting state
		order_types.push_back(order.type);
		order_by_null_types.push_back(order.null_order);
		types.push_back(expr.return_type);
		if (expr.stats) {
			stats.push_back(expr.stats.get());
		} else {
			stats.push_back(nullptr);
		}

		// compute entry size
		if (expr.stats && expr.stats->has_null) {
			entry_size++;
		}
		auto physical_type = expr.return_type.InternalType();
		if (TypeIsConstantSize(physical_type)) {
			entry_size += GetTypeIdSize(expr.return_type.InternalType());
		} else {
			switch (physical_type) {
			default:
				throw NotImplementedException("Variable size sorting type");
			}
		}
	}
	// make room for an 'index' column at the end
	entry_size += sizeof(idx_t);

	state->sorting_state =
	    unique_ptr<SortingState>(new SortingState {entry_size, order_types, order_by_null_types, types, stats});
	idx_t vectors_per_block = (SORTING_BLOCK_SIZE / entry_size + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	vectors_per_block = MaxValue(vectors_per_block, SORTING_BLOCK_SIZE / entry_size / STANDARD_VECTOR_SIZE);
	state->sorting_block = make_unique<RowChunk>(buffer_manager, vectors_per_block * STANDARD_VECTOR_SIZE, entry_size);

	// init payload state and payload block
	entry_size = 0;
	idx_t nullmask_size = (children.size() + 7) / 8;
	entry_size += nullmask_size;
	for (auto &type : children[0]->types) {
		auto physical_type = type.InternalType();
		if (TypeIsConstantSize(physical_type)) {
			entry_size += GetTypeIdSize(physical_type);
		} else {
			switch (physical_type) {
			default:
				throw NotImplementedException("Variable size sorting type");
			}
		}
	}
	state->payload_state = unique_ptr<PayloadState>(new PayloadState {nullmask_size, entry_size});
	vectors_per_block = (SORTING_BLOCK_SIZE / entry_size + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	vectors_per_block = MaxValue(vectors_per_block, SORTING_BLOCK_SIZE / entry_size / STANDARD_VECTOR_SIZE);
	state->payload_block = make_unique<RowChunk>(buffer_manager, vectors_per_block * STANDARD_VECTOR_SIZE, entry_size);

	return state;
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
	auto result = make_unique<OrderLocalState>();
	vector<LogicalType> types;
	for (auto &order : orders) {
		types.push_back(order.expression->return_type);
	}
	result->sort.Initialize(types);
	return result;
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p,
                         DataChunk &input) {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;
	const auto &sorting_state = *gstate.sorting_state;
	const auto &payload_state = *gstate.payload_state;

	if (!lstate.sorting_block) {
		// init using gstate if not initialized yet
		lstate.sorting_block = make_unique<RowChunk>(*gstate.sorting_block);
		lstate.payload_block = make_unique<RowChunk>(*gstate.payload_block);
	}

	// obtain sorting columns
	auto &sort = lstate.sort;
	gstate.executor.Execute(input, sort);

	// build and serialize sorting data
	idx_t start = lstate.sorting_block->Build(sort.size(), lstate.key_locations);
	for (idx_t sort_col = 0; sort_col < sort.data.size(); sort_col++) {
		bool has_null;
		bool invert = false;
		if (sorting_state.STATS[sort_col]) {
			has_null = sorting_state.STATS[sort_col]->has_null;
			invert = sorting_state.ORDER_BY_NULL_TYPES[sort_col] == OrderByNullType::NULLS_LAST;
		} else {
			has_null = false;
		}
		lstate.sorting_block->SerializeVectorSortable(sort.data[sort_col], sort.size(), *lstate.sel_ptr, sort.size(),
		                                              lstate.key_locations, has_null, invert);
	}
	RowChunk::SerializeIndices(lstate.key_locations, start, sort.size());

	// build and serialize payload data
	gstate.payload_block->Build(input.size(), lstate.key_locations);
	for (idx_t i = 0; i < input.size(); i++) {
		memset(lstate.key_locations[i], 0, payload_state.NULLMASK_SIZE);
		lstate.validitymask_locations[i] = lstate.key_locations[i];
		lstate.key_locations[i] += payload_state.NULLMASK_SIZE;
	}
	for (idx_t payl_col = 0; payl_col < input.data.size(); payl_col++) {
		lstate.payload_block->SerializeVector(input.data[payl_col], input.size(), *lstate.sel_ptr, input.size(),
		                                      payl_col, lstate.key_locations, lstate.validitymask_locations);
	}
}

void PhysicalOrder::Combine(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p) {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;

	{
		lock_guard<mutex> append_lock(gstate.lock);
		for (auto &block : lstate.sorting_block->blocks) {
			gstate.sorting_block->count += block.count;
			gstate.sorting_block->blocks.push_back(move(block));
		}
	}
}

static void RadixSort(BufferManager &buffer_manager, data_ptr_t dataptr, const idx_t &count, const idx_t &col_offset,
                      const idx_t &sorting_size, const SortingState &sorting_state) {
	auto temp_block = buffer_manager.RegisterMemory(
	    MaxValue(count * sorting_state.ENTRY_SIZE, (idx_t)Storage::BLOCK_ALLOC_SIZE), false);
	auto handle = buffer_manager.Pin(temp_block);
	data_ptr_t temp = handle->node->buffer;
	bool swap = false;

	idx_t counts[256];
	u_int8_t byte;
	for (idx_t offset = col_offset + sorting_size - 1; offset + 1 > col_offset; offset--) {
		// init to 0
		memset(counts, 0, sizeof(counts));
		// collect counts
		for (idx_t i = 0; i < count; i++) {
			byte = *(dataptr + i * sorting_state.ENTRY_SIZE + offset);
			counts[byte]++;
		}
		// compute offsets from counts
		for (idx_t val = 1; val < 256; val++) {
			counts[val] = counts[val] + counts[val - 1];
		}
		// re-order the data in temporary array
		for (int i = count - 1; i >= 0; i--) {
			byte = *(dataptr + i * sorting_state.ENTRY_SIZE + offset);
			memcpy(temp + (counts[byte] - 1) * sorting_state.ENTRY_SIZE, dataptr + i * sorting_state.ENTRY_SIZE,
			       sorting_state.ENTRY_SIZE);
			counts[byte]--;
		}
		std::swap(dataptr, temp);
		swap = !swap;
	}

	if (swap) {
		memcpy(temp, dataptr, count * sorting_state.ENTRY_SIZE);
	}
}

static void SortInMemory(Pipeline &pipeline, ClientContext &context, OrderGlobalState &state) {
	const auto &sorting_state = *state.sorting_state;
	auto &buffer_manager = BufferManager::GetBufferManager(context);

	auto &block = state.sorting_block->blocks.back();
	auto handle = buffer_manager.Pin(block.block);
	auto dataptr = handle->node->buffer;

	const idx_t sorting_size = sorting_state.ENTRY_SIZE - sizeof(idx_t);
	RadixSort(buffer_manager, dataptr, block.count, 0, sorting_size, sorting_state);
}

void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state_p) {
	this->sink_state = move(state_p);
	auto &state = (OrderGlobalState &)*this->sink_state;
	const auto &sorting_state = *state.sorting_state;
	const auto &payload_state = *state.payload_state;

	idx_t total_size = state.payload_block->count * payload_state.ENTRY_SIZE;
	if (total_size > state.buffer_manager.GetMaxMemory() / 2) {
		throw NotImplementedException("External sort");
	}

	if (state.sorting_block->blocks.size() > 1) {
		// copy all of the sorting data to one big block
		idx_t capacity =
		    MaxValue(Storage::BLOCK_ALLOC_SIZE / sorting_state.ENTRY_SIZE + 1, total_size / sorting_state.ENTRY_SIZE);
		RowDataBlock new_block(state.buffer_manager, capacity, sorting_state.ENTRY_SIZE);
		auto new_block_handle = state.buffer_manager.Pin(new_block.block);
		data_ptr_t new_block_ptr = new_block_handle->node->buffer;
		for (auto &block : state.sorting_block->blocks) {
			auto block_handle = state.buffer_manager.Pin(block.block);
			memcpy(new_block_ptr, block_handle->node->buffer, block.count * sorting_state.ENTRY_SIZE);
			new_block_ptr += block.count * sorting_state.ENTRY_SIZE;
			state.buffer_manager.UnregisterBlock(block.block->BlockId(), true);
		}
		state.sorting_block->blocks.clear();
		state.sorting_block->block_capacity = capacity;
		state.sorting_block->blocks.push_back(move(new_block));
	}

	if (state.payload_block->blocks.size() > 1) {
		// same for the payload data
		idx_t capacity =
		    MaxValue(Storage::BLOCK_ALLOC_SIZE / payload_state.ENTRY_SIZE + 1, total_size / payload_state.ENTRY_SIZE);
		RowDataBlock new_block(state.buffer_manager, capacity, payload_state.ENTRY_SIZE);
		auto new_block_handle = state.buffer_manager.Pin(new_block.block);
		data_ptr_t new_block_ptr = new_block_handle->node->buffer;
		for (auto &block : state.payload_block->blocks) {
			auto block_handle = state.buffer_manager.Pin(block.block);
			memcpy(new_block_ptr, block_handle->node->buffer, block.count * payload_state.ENTRY_SIZE);
			new_block_ptr += block.count * payload_state.ENTRY_SIZE;
			state.buffer_manager.UnregisterBlock(block.block->BlockId(), true);
		}
		state.payload_block->blocks.clear();
		state.payload_block->block_capacity = capacity;
		state.payload_block->blocks.push_back(move(new_block));
	}

	SortInMemory(pipeline, context, state);
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderOperatorState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), entry_idx(0), count(-1) {
	}
	unique_ptr<BufferHandle> sorting_handle = nullptr;
	unique_ptr<BufferHandle> payload_handle;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t validitymask_locations[STANDARD_VECTOR_SIZE];

	idx_t entry_idx;
	idx_t count;
};

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(*this, children[0].get());
}

void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto &state = *reinterpret_cast<PhysicalOrderOperatorState *>(state_p);
	auto &gstate = (OrderGlobalState &)*this->sink_state;
	const auto &sorting_state = *gstate.sorting_state;
	const auto &payload_state = *gstate.payload_state;

	if (gstate.sorting_block->blocks.empty() || state.entry_idx >= state.count) {
		state.finished = true;
		return;
	}

	if (!state.sorting_handle) {
		state.sorting_handle = gstate.buffer_manager.Pin(gstate.sorting_block->blocks[0].block);
		state.payload_handle = gstate.buffer_manager.Pin(gstate.payload_block->blocks[0].block);
		state.count = gstate.sorting_block->count;
	}

	// fetch the next batch of pointers from the block
	const idx_t next = MinValue((idx_t)STANDARD_VECTOR_SIZE, state.count - state.entry_idx);
	data_ptr_t sort_dataptr = state.sorting_handle->node->buffer + (state.entry_idx * sorting_state.ENTRY_SIZE) +
	                          sorting_state.ENTRY_SIZE - sizeof(idx_t);
	const data_ptr_t payl_dataptr = state.payload_handle->node->buffer;
	for (idx_t i = 0; i < next; i++) {
		state.validitymask_locations[i] = payl_dataptr + Load<idx_t>(sort_dataptr) * payload_state.ENTRY_SIZE;
		state.key_locations[i] = state.validitymask_locations[i] + payload_state.NULLMASK_SIZE;
		sort_dataptr += sorting_state.ENTRY_SIZE;
	}

	// deserialize the payload data
	for (idx_t payl_col = 0; payl_col < chunk.data.size(); payl_col++) {
		RowChunk::DeserializeIntoVector(chunk.data[payl_col], next, payl_col, state.key_locations,
		                                state.validitymask_locations);
	}
	state.entry_idx += STANDARD_VECTOR_SIZE;
	chunk.SetCardinality(next);
	chunk.Verify();
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
