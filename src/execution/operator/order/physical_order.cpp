#include "duckdb/execution/operator/order/physical_order.hpp"

#include "blockquicksort_wrapper.hpp"
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
	const bool HAS_VARIABLE_SIZE;
	const idx_t NULLMASK_SIZE;
	const idx_t ENTRY_SIZE;
};

class OrderGlobalState : public GlobalOperatorState {
public:
	explicit OrderGlobalState(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
	}
	//! The lock for updating the global order state
	mutex lock;

	BufferManager &buffer_manager;
	unique_ptr<RowChunk> sorting_block;
	unique_ptr<RowChunk> payload_block;
	unique_ptr<RowChunk> sizes_block;

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
	unique_ptr<RowChunk> sizes_block = nullptr;

	//! Used for vector serialization
	const SelectionVector *sel_ptr = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t validitymask_locations[STANDARD_VECTOR_SIZE];
	idx_t entry_sizes[STANDARD_VECTOR_SIZE];
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
		if (!expr.stats || expr.stats->has_null) {
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
	idx_t vectors_per_block =
	    (Storage::BLOCK_ALLOC_SIZE / entry_size + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	state->sorting_block = make_unique<RowChunk>(buffer_manager, vectors_per_block * STANDARD_VECTOR_SIZE, entry_size);

	// init payload state
	entry_size = 0;
	idx_t nullmask_size = (children.size() + 7) / 8;
	entry_size += nullmask_size;
	bool variable_payload_size = false;
	for (auto &type : children[0]->types) {
		auto physical_type = type.InternalType();
		if (TypeIsConstantSize(physical_type)) {
			entry_size += GetTypeIdSize(physical_type);
		} else {
			variable_payload_size = true;
			// we keep track of the 'base size' of variable size payload entries
			switch (physical_type) {
			case PhysicalType::VARCHAR:
				entry_size += string_t::PREFIX_LENGTH;
				break;
			default:
				throw NotImplementedException("Variable size payload type");
			}
		}
	}
	state->payload_state =
	    unique_ptr<PayloadState>(new PayloadState {variable_payload_size, nullmask_size, entry_size});
	vectors_per_block = (Storage::BLOCK_ALLOC_SIZE / entry_size + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	state->payload_block = make_unique<RowChunk>(buffer_manager, vectors_per_block * STANDARD_VECTOR_SIZE, entry_size);

	if (variable_payload_size) {
		// if payload entry size is not constant, we keep track of entry sizes
		state->sizes_block =
		    make_unique<RowChunk>(buffer_manager, (idx_t)Storage::BLOCK_ALLOC_SIZE / sizeof(idx_t) + 1, sizeof(idx_t));
	}

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

static void ComputeEntrySizes(DataChunk &input, idx_t entry_sizes[], const PayloadState &payload_state) {
	// fill array with constant portion of payload entry size
	std::fill_n(entry_sizes, input.size(), payload_state.ENTRY_SIZE);

	// compute size of the constant portion of the payload columns
	VectorData vdata;
	for (idx_t payl_idx = 0; payl_idx < input.data.size(); payl_idx++) {
		auto physical_type = input.data[payl_idx].GetType().InternalType();
		if (TypeIsConstantSize(physical_type)) {
			continue;
		}
		input.data[payl_idx].Orrify(input.size(), vdata);
		switch (physical_type) {
		case PhysicalType::VARCHAR: {
			auto strings = (string_t *)vdata.data;
			for (idx_t i = 0; i < input.size(); i++) {
				entry_sizes[i] += strings[vdata.sel->get_index(i)].GetSize();
			}
			break;
		}
		default:
			throw NotImplementedException("Variable size type not implemented for sorting!");
		}
	}
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
		if (payload_state.HAS_VARIABLE_SIZE) {
			lstate.sizes_block = make_unique<RowChunk>(*gstate.sizes_block);
		}
	}

	// obtain sorting columns
	auto &sort = lstate.sort;
	gstate.executor.Execute(input, sort);

	// build and serialize sorting data
	lstate.sorting_block->Build(sort.size(), lstate.key_locations, nullptr);
	for (idx_t sort_col = 0; sort_col < sort.data.size(); sort_col++) {
		bool has_null = sorting_state.STATS[sort_col] ? sorting_state.STATS[sort_col]->has_null : true;
		bool nulls_first = sorting_state.ORDER_BY_NULL_TYPES[sort_col] == OrderByNullType::NULLS_FIRST;
		bool desc = sorting_state.ORDER_TYPES[sort_col] == OrderType::DESCENDING;
		lstate.sorting_block->SerializeVectorSortable(sort.data[sort_col], sort.size(), *lstate.sel_ptr, sort.size(),
		                                              lstate.key_locations, desc, has_null, nulls_first);
	}

	// compute entry sizes of payload columns if there are variable size columns
	if (payload_state.HAS_VARIABLE_SIZE) {
		ComputeEntrySizes(input, lstate.entry_sizes, payload_state);
		lstate.sizes_block->Build(input.size(), lstate.key_locations, nullptr);
		for (idx_t i = 0; i < input.size(); i++) {
			Store<idx_t>(lstate.entry_sizes[i], lstate.key_locations[i]);
		}
	}

	// build and serialize payload data
	gstate.payload_block->Build(input.size(), lstate.key_locations, lstate.entry_sizes);
	for (idx_t i = 0; i < input.size(); i++) {
		memset(lstate.key_locations[i], -1, payload_state.NULLMASK_SIZE);
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
	const auto &payload_state = *gstate.payload_state;

	lock_guard<mutex> append_lock(gstate.lock);
	for (auto &block : lstate.sorting_block->blocks) {
		gstate.sorting_block->count += block.count;
		gstate.sorting_block->blocks.push_back(move(block));
	}
	for (auto &block : lstate.payload_block->blocks) {
		gstate.payload_block->count += block.count;
		gstate.payload_block->blocks.push_back(move(block));
	}
	if (payload_state.HAS_VARIABLE_SIZE) {
		for (auto &block : lstate.sizes_block->blocks) {
			gstate.sizes_block->count += block.count;
			gstate.sizes_block->blocks.push_back(move(block));
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

	// assign an index to each row
	const idx_t sorting_size = sorting_state.ENTRY_SIZE - sizeof(idx_t);
	data_ptr_t idx_dataptr = dataptr + sorting_size;
	for (idx_t i = 0; i < block.count; i++) {
		Store<idx_t>(i, idx_dataptr);
		idx_dataptr += sorting_state.ENTRY_SIZE;
	}

	// now sort the data
	RadixSort(buffer_manager, dataptr, block.count, 0, sorting_size, sorting_state);
}

void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state_p) {
	this->sink_state = move(state_p);
	auto &state = (OrderGlobalState &)*this->sink_state;
	const auto &sorting_state = *state.sorting_state;
	const auto &payload_state = *state.payload_state;
	D_ASSERT(state.sorting_block->count == state.payload_block->count);

	if (state.sorting_block->count == 0) {
		return;
	}

	idx_t total_size = state.payload_block->count * payload_state.ENTRY_SIZE;
	if (total_size > state.buffer_manager.GetMaxMemory() / 2) {
		throw NotImplementedException("External sort");
	}

	// copy all of the sorting data to one big block
	idx_t s_capacity = MaxValue(Storage::BLOCK_ALLOC_SIZE / sorting_state.ENTRY_SIZE + 1, state.sorting_block->count);
	RowDataBlock s_block(state.buffer_manager, s_capacity, sorting_state.ENTRY_SIZE);
	s_block.count = state.sorting_block->count;
	auto s_block_handle = state.buffer_manager.Pin(s_block.block);
	data_ptr_t s_block_ptr = s_block_handle->node->buffer;
	for (auto &block : state.sorting_block->blocks) {
		auto block_handle = state.buffer_manager.Pin(block.block);
		memcpy(s_block_ptr, block_handle->node->buffer, block.count * sorting_state.ENTRY_SIZE);
		s_block_ptr += block.count * sorting_state.ENTRY_SIZE;
		state.buffer_manager.UnregisterBlock(block.block->BlockId(), true);
	}
	state.sorting_block->blocks.clear();
	state.sorting_block->block_capacity = s_capacity;
	state.sorting_block->blocks.push_back(move(s_block));

	// same for the payload data
	idx_t p_capacity = MaxValue(Storage::BLOCK_ALLOC_SIZE / payload_state.ENTRY_SIZE + 1, state.payload_block->count);
	RowDataBlock p_block(state.buffer_manager, p_capacity, payload_state.ENTRY_SIZE);
	p_block.count = state.payload_block->count;
	auto p_block_handle = state.buffer_manager.Pin(p_block.block);
	data_ptr_t p_block_ptr = p_block_handle->node->buffer;
	for (auto &block : state.payload_block->blocks) {
		auto block_handle = state.buffer_manager.Pin(block.block);
		if (payload_state.HAS_VARIABLE_SIZE) {
			memcpy(p_block_ptr, block_handle->node->buffer, block.byte_offset);
			p_block_ptr += block.byte_offset;
		} else {
			memcpy(p_block_ptr, block_handle->node->buffer, block.count * payload_state.ENTRY_SIZE);
			p_block_ptr += block.count * payload_state.ENTRY_SIZE;
		}
		state.buffer_manager.UnregisterBlock(block.block->BlockId(), true);
	}
	state.payload_block->blocks.clear();
	state.payload_block->block_capacity = p_capacity;
	state.payload_block->blocks.push_back(move(p_block));

	if (payload_state.HAS_VARIABLE_SIZE) {
		D_ASSERT(state.sizes_block->count == state.sorting_block->count);
		idx_t sz_capacity =
		    MaxValue(Storage::BLOCK_ALLOC_SIZE / state.sizes_block->entry_size + 1, state.sizes_block->count + 1);
		RowDataBlock sz_block(state.buffer_manager, sz_capacity, state.sizes_block->entry_size);
		sz_block.count = state.sizes_block->count;
		auto sz_block_handle = state.buffer_manager.Pin(sz_block.block);
		data_ptr_t sz_block_ptr = sz_block_handle->node->buffer;
		for (auto &block : state.sizes_block->blocks) {
			auto block_handle = state.buffer_manager.Pin(block.block);
			memcpy(sz_block_ptr, block_handle->node->buffer, block.count * state.sizes_block->entry_size);
			sz_block_ptr += block.count * state.sizes_block->entry_size;
			state.buffer_manager.UnregisterBlock(block.block->BlockId(), true);
		}
		state.sizes_block->blocks.clear();
		state.sizes_block->block_capacity = sz_capacity;
		// convert sizes to offsets
		idx_t *offsets = (idx_t *)sz_block_handle->node->buffer;
		idx_t prev = offsets[0];
		offsets[0] = 0;
		idx_t curr;
		for (idx_t i = 1; i < state.sizes_block->count; i++) {
			curr = offsets[i];
			offsets[i] = offsets[i - 1] + prev;
			prev = curr;
		}
		offsets[state.sizes_block->count] = offsets[state.sizes_block->count - 1] + prev;
		state.sizes_block->blocks.push_back(move(sz_block));
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
	unique_ptr<BufferHandle> offsets_handle;

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
		if (payload_state.HAS_VARIABLE_SIZE) {
			state.offsets_handle = gstate.buffer_manager.Pin(gstate.sizes_block->blocks[0].block);
		}
	}

	// fetch the next batch of pointers from the block
	const idx_t next = MinValue((idx_t)STANDARD_VECTOR_SIZE, state.count - state.entry_idx);
	data_ptr_t sort_dataptr = state.sorting_handle->node->buffer + (state.entry_idx * sorting_state.ENTRY_SIZE) +
	                          sorting_state.ENTRY_SIZE - sizeof(idx_t);
	const data_ptr_t payl_dataptr = state.payload_handle->node->buffer;
	if (payload_state.HAS_VARIABLE_SIZE) {
		idx_t *offsets = (idx_t *)state.offsets_handle->node->buffer;
		for (idx_t i = 0; i < next; i++) {
			state.validitymask_locations[i] = payl_dataptr + offsets[Load<idx_t>(sort_dataptr)];
			state.key_locations[i] = state.validitymask_locations[i] + payload_state.NULLMASK_SIZE;
			sort_dataptr += sorting_state.ENTRY_SIZE;
		}
	} else {
		for (idx_t i = 0; i < next; i++) {
			state.validitymask_locations[i] = payl_dataptr + Load<idx_t>(sort_dataptr) * payload_state.ENTRY_SIZE;
			state.key_locations[i] = state.validitymask_locations[i] + payload_state.NULLMASK_SIZE;
			sort_dataptr += sorting_state.ENTRY_SIZE;
		}
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
