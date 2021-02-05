#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/row_chunk.hpp"
#include "duckdb/main/client_context.hpp"
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
	OrderGlobalState(PhysicalOrder &_op, BufferManager &buffer_manager)
	    : op(_op), sorting(buffer_manager), payload(buffer_manager) {
	}
	PhysicalOrder &op;

	//! The lock for updating the global aggregate state
	mutex lock;

	//! Types of the sorting columns
	vector<PhysicalType> sorting_types;
	//! Sizes of the sorting columns
	vector<idx_t> sorting_sizes;
	//! To execute the expressions that are sorted
	ExpressionExecutor executor;
	//! Sorting columns in row format
	RowChunk sorting;
	//! Payload in row format
	RowChunk payload;
};

class OrderLocalState : public LocalSinkState {
public:
	OrderLocalState(BufferManager &buffer_manager) : sorting(buffer_manager), payload(buffer_manager) {
	}

	//! Buffers to hold chunks
	ChunkCollection sorting_buffer;
	ChunkCollection payload_buffer;
	//! Data in row format
	RowChunk sorting;
	RowChunk payload;

	// allocate in order to data to the chunks
	const SelectionVector *sel_ptr = &FlatVector::IncrementalSelectionVector;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t nullmask_locations[STANDARD_VECTOR_SIZE];

	void Flush(RowChunk &sorting, RowChunk &payload) {
		// sink local sorting buffer into the given RowChunk
		for (auto next_chunk = sorting_buffer.Fetch(); next_chunk != nullptr; next_chunk = sorting_buffer.Fetch()) {
			// TODO: convert this to a bit representation that is sortable by memcmp?
			//  using 'orders' - this would reduce branch predictions in inner loop 'compare_tuple'
			sorting.Build(next_chunk->size(), key_locations, nullmask_locations);
			for (idx_t i = 0; i < next_chunk->data.size(); i++) {
				sorting.SerializeVector(next_chunk->data[i], next_chunk->size(), *sel_ptr, next_chunk->size(), i,
				                        key_locations, nullmask_locations);
			}
		}
		sorting_buffer.Reset();

		// sink local payload buffer into the given RowChunk
		for (auto next_chunk = payload_buffer.Fetch(); next_chunk != nullptr; next_chunk = payload_buffer.Fetch()) {
			payload.Build(next_chunk->size(), key_locations, nullmask_locations);
			for (idx_t i = 0; i < next_chunk->data.size(); i++) {
				payload.SerializeVector(next_chunk->data[i], next_chunk->size(), *sel_ptr, next_chunk->size(), i,
				                        key_locations, nullmask_locations);
			}
		}
		payload_buffer.Reset();
	}
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
	auto state = make_unique<OrderGlobalState>(*this, BufferManager::GetBufferManager(context));
	for (auto &order : orders) {
		state->sorting_types.push_back(order.expression->return_type.InternalType());
		state->sorting_sizes.push_back(GetTypeIdSize(order.expression->return_type.InternalType()));
		state->executor.AddExpression(*order.expression);
	}
	return state;
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
	auto state = make_unique<OrderLocalState>(BufferManager::GetBufferManager(context.client));

	// initialize sorting
	auto &sorting = state->sorting;
	sorting.nullmask_size = (orders.size() + 7) / 8;
	sorting.entry_size += sorting.nullmask_size;
	for (auto &order : orders) {
		sorting.entry_size += GetTypeIdSize(order.expression->return_type.InternalType());
	}

	// initialize payload
	auto &payload = state->payload;
	payload.nullmask_size = (types.size() + 7) / 8;
	payload.entry_size += payload.nullmask_size;
	for (auto &type : types) {
		payload.entry_size += GetTypeIdSize(type.InternalType());
	}

	// initialize block capacities
	auto min_entry_size = MinValue<idx_t>(sorting.entry_size, payload.entry_size);
	idx_t vectors_per_block = (SORTING_BLOCK_SIZE / min_entry_size + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	sorting.block_capacity = vectors_per_block * STANDARD_VECTOR_SIZE;
	payload.block_capacity = sorting.block_capacity;

	return state;
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_,
                         DataChunk &input) {
	auto &gstate = (OrderGlobalState &)state;
	auto &lstate = (OrderLocalState &)lstate_;

	// obtain sorting columns
	vector<LogicalType> sort_types;
	for (auto &order : orders) {
		sort_types.push_back(order.expression->return_type);
	}
	DataChunk sort;
	sort.Initialize(sort_types);
	gstate.executor.Execute(input, sort);

	// store chunks in buffer
	lstate.sorting_buffer.Append(sort);
	lstate.payload_buffer.Append(input);

	// flush buffers into row format if the next block should be filled
	idx_t last_block_count = lstate.sorting.blocks.empty() ? 0 : lstate.sorting.blocks.back().count;
	if (last_block_count + lstate.sorting_buffer.Count() + STANDARD_VECTOR_SIZE > lstate.sorting.block_capacity) {
		lstate.Flush(lstate.sorting, lstate.payload);
	}
}

void PhysicalOrder::Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_) {
	auto &gstate = (OrderGlobalState &)state;
	auto &lstate = (OrderLocalState &)lstate_;

	lock_guard<mutex> glock(gstate.lock);
	if (gstate.sorting.blocks.empty()) {
		gstate.sorting.Append(lstate.sorting);
		gstate.payload.Append(lstate.payload);
	} else {
		// remove last (possibly not full) block
		auto sorting_back = gstate.sorting.blocks.back();
		auto payload_back = gstate.payload.blocks.back();
		gstate.sorting.blocks.pop_back();
		gstate.payload.blocks.pop_back();

		// append full blocks
		gstate.sorting.Append(lstate.sorting);
		gstate.payload.Append(lstate.payload);

		// re-add removed blocks
		gstate.sorting.blocks.push_back(sorting_back);
		gstate.payload.blocks.push_back(payload_back);
	}

	// flush remaining buffers into global state
	if (lstate.sorting_buffer.Count() > 0) {
		lstate.Flush(gstate.sorting, gstate.payload);
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
template <class TYPE>
static int8_t templated_compare_value(data_ptr_t &l_val, data_ptr_t &r_val) {
	auto left_val = Load<TYPE>(l_val);
	auto right_val = Load<TYPE>(r_val);
	if (Equals::Operation<TYPE>(left_val, right_val)) {
		return 0;
	}
	if (LessThan::Operation<TYPE>(left_val, right_val)) {
		return -1;
	}
	return 1;
}

static int32_t compare_value(data_ptr_t &l_nullmask, data_ptr_t &r_nullmask, data_ptr_t &l_val, data_ptr_t &r_val,
                             const idx_t &sort_idx, OrderGlobalState &state) {
	bool left_null = *l_nullmask & (1 << sort_idx);
	bool right_null = *r_nullmask & (1 << sort_idx);

	if (left_null && right_null) {
		return 0;
	} else if (right_null) {
		return state.op.orders[sort_idx].null_order == OrderByNullType::NULLS_FIRST ? 1 : -1;
	} else if (left_null) {
		return state.op.orders[sort_idx].null_order == OrderByNullType::NULLS_FIRST ? -1 : 1;
	}

	switch (state.sorting_types[sort_idx]) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return templated_compare_value<int8_t>(l_val, r_val);
	case PhysicalType::INT16:
		return templated_compare_value<int16_t>(l_val, r_val);
	case PhysicalType::INT32:
		return templated_compare_value<int32_t>(l_val, r_val);
	case PhysicalType::INT64:
		return templated_compare_value<int64_t>(l_val, r_val);
	case PhysicalType::UINT8:
		return templated_compare_value<uint8_t>(l_val, r_val);
	case PhysicalType::UINT16:
		return templated_compare_value<uint16_t>(l_val, r_val);
	case PhysicalType::UINT32:
		return templated_compare_value<uint32_t>(l_val, r_val);
	case PhysicalType::UINT64:
		return templated_compare_value<uint64_t>(l_val, r_val);
	case PhysicalType::INT128:
		return templated_compare_value<hugeint_t>(l_val, r_val);
	case PhysicalType::FLOAT:
		return templated_compare_value<float>(l_val, r_val);
	case PhysicalType::DOUBLE:
		return templated_compare_value<double>(l_val, r_val);
	case PhysicalType::VARCHAR:
		return templated_compare_value<string_t>(l_val, r_val);
	case PhysicalType::INTERVAL:
		return templated_compare_value<interval_t>(l_val, r_val);
	default:
		throw NotImplementedException("Type for comparison");
	}
}

static int compare_tuple(data_ptr_t &l_start, data_ptr_t &r_start, OrderGlobalState &state) {
	auto l_val = l_start + state.sorting.nullmask_size;
	auto r_val = r_start + state.sorting.nullmask_size;
	for (idx_t i = 0; i < state.op.orders.size(); i++) {
		auto comp_res = compare_value(l_start, r_start, l_val, r_val, i, state);
		if (comp_res == 0) {
			l_val += state.sorting_sizes[i];
			r_val += state.sorting_sizes[i];
			continue;
		}
		return comp_res < 0 ? (state.op.orders[i].type == OrderType::ASCENDING ? -1 : 1)
		                    : (state.op.orders[i].type == OrderType::ASCENDING ? 1 : -1);
	}
	return 0;
}

class PhysicalOrderSortTask : public Task {
public:
	PhysicalOrderSortTask(Pipeline &parent_, OrderGlobalState &state_, BufferManager &buffer_manager_, idx_t block_idx_)
	    : parent(parent_), state(state_), buffer_manager(buffer_manager_), block_idx(block_idx_),
	      sort(state.sorting.blocks[block_idx]), payl(state.payload.blocks[block_idx]) {
	}

	void Execute() override {
		// get sorting data from the buffer manager
		auto handle = buffer_manager.Pin(sort.block);
		auto dataptr = handle->node->buffer;

		// fetch a batch of pointers to entries in the blocks
		auto key_locations = unique_ptr<data_ptr_t[]>(new data_ptr_t[sort.count]);
		auto idxs = unique_ptr<idx_t[]>(new idx_t[sort.count]);
		for (idx_t i = 0; i < sort.count; i++) {
			key_locations[i] = dataptr;
			idxs[i] = i;
			dataptr += state.sorting.entry_size;
		}

		// sort the indices
		Sort(key_locations.get(), idxs.get());

		// re-order sorting data
		ReOrder(state.sorting, key_locations.get(), idxs.get());

		// now re-order payload data
		handle = buffer_manager.Pin(payl.block);
		dataptr = handle->node->buffer;
		for (idx_t i = 0; i < payl.count; i++) {
			key_locations[i] = dataptr;
			dataptr += state.payload.entry_size;
		}
		ReOrder(state.payload, key_locations.get(), idxs.get());

		// finish task
		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
	}

private:
	void Sort(data_ptr_t key_locations[], idx_t idxs[]) {
		// create reference so it can be used in the lambda function
		OrderGlobalState &state_ref = state;
		std::sort(idxs, idxs + sort.count, [key_locations, &state_ref](const idx_t &l_i, const idx_t &r_i) {
			return compare_tuple(key_locations[l_i], key_locations[r_i], state_ref) <= 0;
		});
	}

	void ReOrder(RowChunk &chunk, data_ptr_t key_locations[], const idx_t idxs[]) {
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
			memcpy(dataptr + idxs[i] * chunk.entry_size, key_locations[i], chunk.entry_size);
		}

		// replace old block with new block, and unregister the old block
		chunk.blocks[block_idx] = new_block;
		// FIXME: does this need to be unregistered manually?
		//		buffer_manager.UnregisterBlock(old_block.block->BlockId(), true);
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
	PhysicalOrderMergeTask(Pipeline &parent_, OrderGlobalState &state_, BufferManager &buffer_manager_,
	                       RowDataBlock &sort_l_, RowDataBlock &sort_r_, RowDataBlock &payl_l_, RowDataBlock &payl_r_)
	    : parent(parent_), state(state_), buffer_manager(buffer_manager_), sort_l(sort_l_), sort_r(sort_r_),
	      payl_l(payl_l_), payl_r(payl_r_) {
	}

	void Execute() override {
		auto l_handle = buffer_manager.Pin(sort_l.block);
		auto l_dataptr = l_handle->node->buffer;
		auto l_locations = unique_ptr<data_ptr_t[]>(new data_ptr_t[sort_l.count]);
		for (idx_t i = 0; i < sort_l.count; i++) {
			l_locations[i] = l_dataptr;
			l_dataptr += state.sorting.entry_size;
		}

		auto r_handle = buffer_manager.Pin(sort_r.block);
		auto r_dataptr = r_handle->node->buffer;
		auto r_locations = unique_ptr<data_ptr_t[]>(new data_ptr_t[sort_r.count]);
		for (idx_t i = 0; i < sort_r.count; i++) {
			r_locations[i] = r_dataptr;
			r_dataptr += state.sorting.entry_size;
		}

		auto l_idxs = unique_ptr<idx_t[]>(new idx_t[sort_l.count]);
		auto r_idxs = unique_ptr<idx_t[]>(new idx_t[sort_r.count]);
		Merge(l_locations.get(), r_locations.get(), l_idxs.get(), r_idxs.get());

		RowDataBlock sort_merged;
		sort_merged.count = sort_l.count + sort_r.count;
		sort_merged.capacity = sort_l.capacity + sort_r.capacity;
		sort_merged.block = buffer_manager.RegisterMemory(sort_merged.capacity * state.sorting.entry_size, false);
		auto sort_merged_handle = buffer_manager.Pin(sort_merged.block);
		auto sort_merged_dataptr = sort_merged_handle->node->buffer;

		ReOrder(l_idxs.get(), r_idxs.get(), l_locations.get(), r_locations.get(), sort_merged_dataptr);

		l_handle = buffer_manager.Pin(payl_l.block);
		l_dataptr = l_handle->node->buffer;
		for (idx_t i = 0; i < payl_l.count; i++) {
			l_locations[i] = l_dataptr;
			l_dataptr += state.payload.entry_size;
		}

		r_handle = buffer_manager.Pin(payl_r.block);
		r_dataptr = r_handle->node->buffer;
		for (idx_t i = 0; i < payl_r.count; i++) {
			r_locations[i] = r_dataptr;
			r_dataptr += state.payload.entry_size;
		}

		RowDataBlock payl_merged;
		payl_merged.count = payl_l.count + payl_r.count;
		payl_merged.capacity = payl_l.capacity + payl_r.capacity;
		payl_merged.block = buffer_manager.RegisterMemory(payl_merged.capacity * state.payload.entry_size, false);
		auto payl_merged_handle = buffer_manager.Pin(payl_merged.block);
		auto payl_merged_dataptr = payl_merged_handle->node->buffer;

		ReOrder(l_idxs.get(), r_idxs.get(), l_locations.get(), r_locations.get(), payl_merged_dataptr);

		lock_guard<mutex> glock(state.lock);
		state.sorting.blocks.push_back(sort_merged);
		state.payload.blocks.push_back(payl_merged);
		parent.finished_tasks++;
		// finish the whole pipeline
		if (parent.total_tasks == parent.finished_tasks) {
			parent.Finish();
		}
	}

private:
	void Merge(data_ptr_t l_locations[], data_ptr_t r_locations[], idx_t l_idxs[], idx_t r_idxs[]) {
		idx_t l_offset = 0;
		idx_t r_offset = 0;
		while (l_offset < sort_l.count || r_offset < sort_r.count) {
			for (; l_offset < sort_l.count && compare_tuple(l_locations[l_offset], r_locations[r_offset], state) <= 0;
			     l_offset++) {
				l_idxs[l_offset] = l_offset + r_offset;
			}
			for (; r_offset < sort_r.count && compare_tuple(r_locations[r_offset], l_locations[l_offset], state) <= 0;
			     r_offset++) {
				r_idxs[r_offset] = r_offset + l_offset;
			}
		}
	}

	void ReOrder(idx_t l_idxs[], idx_t r_idxs[], data_ptr_t l_locations[], data_ptr_t r_locations[],
	             data_ptr_t merged) {
		for (idx_t i = 0; i < sort_l.count; i++) {
			memcpy(merged + l_idxs[i] * state.sorting.entry_size, l_locations[i], state.sorting.entry_size);
		}
		for (idx_t i = 0; i < sort_r.count; i++) {
			memcpy(merged + r_idxs[i] * state.sorting.entry_size, r_locations[i], state.sorting.entry_size);
		}
	}

	Pipeline &parent;
	OrderGlobalState &state;
	BufferManager &buffer_manager;

	RowDataBlock sort_l;
	RowDataBlock sort_r;
	RowDataBlock payl_l;
	RowDataBlock payl_r;
};

void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	// finalize: perform the actual sorting
	auto &sink = (OrderGlobalState &)*state;

	// schedule sorting tasks for each block
	for (idx_t i = 0; i < sink.payload.blocks.size(); i++) {
		auto new_task = make_unique<PhysicalOrderSortTask>(pipeline, sink, BufferManager::GetBufferManager(context), i);
		TaskScheduler::GetScheduler(context).ScheduleTask(pipeline.token, move(new_task));
	}

	while (sink.payload.blocks.size() > 1) {
		while (sink.payload.blocks.size() > 1) {
			auto s_l = sink.sorting.blocks.erase(sink.sorting.blocks.begin());
			auto s_r = sink.sorting.blocks.erase(sink.sorting.blocks.begin());
			auto p_l = sink.payload.blocks.erase(sink.payload.blocks.begin());
			auto p_r = sink.payload.blocks.erase(sink.payload.blocks.begin());
			auto new_task = make_unique<PhysicalOrderMergeTask>(
			    pipeline, sink, BufferManager::GetBufferManager(context), *s_l, *s_r, *p_l, *p_r);
			TaskScheduler::GetScheduler(context).ScheduleTask(pipeline.token, move(new_task));
		}
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
		// FIXME: does this need to be unregistered manually?
		//		sink.payload.buffer_manager.UnregisterBlock(sink.payload.blocks[state->current_block].block->BlockId(),
		// true);
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
