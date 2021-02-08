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

		// fetch a batch of pointers to entries in the blocks, and initialize idxs
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
			memcpy(dataptr + i * chunk.entry_size, key_locations[idxs[i]], chunk.entry_size);
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
	                       vector<std::pair<RowDataBlock *, RowDataBlock *>> l_blocks_,
	                       vector<std::pair<RowDataBlock *, RowDataBlock *>> r_blocks_,
	                       vector<std::pair<RowDataBlock, RowDataBlock>> &result_, idx_t l_start_, idx_t r_start_,
	                       idx_t l_end_, idx_t r_end_)
	    : parent(parent_), state(state_), buffer_manager(buffer_manager_), l_blocks(l_blocks_), r_blocks(r_blocks_),
	      result(result_), l_start(l_start_), r_start(r_start_), l_end(l_end_), r_end(r_end_) {
	}

	void Execute() override {
		idx_t l_block_idx = 0, r_block_idx = 0;

		auto l_sort_handle = buffer_manager.Pin(l_blocks[l_block_idx].first->block);
		auto l_payl_handle = buffer_manager.Pin(l_blocks[l_block_idx].second->block);
		auto r_sort_handle = buffer_manager.Pin(r_blocks[r_block_idx].first->block);
		auto r_payl_handle = buffer_manager.Pin(r_blocks[r_block_idx].second->block);
		auto l_sort_ptr = l_sort_handle->node->buffer + l_start * state.sorting.entry_size;
		auto l_payl_ptr = l_payl_handle->node->buffer + l_start * state.payload.entry_size;
		auto r_sort_ptr = r_sort_handle->node->buffer + r_start * state.sorting.entry_size;
		auto r_payl_ptr = r_payl_handle->node->buffer + r_start * state.payload.entry_size;

		idx_t l_offset = 0, r_offset = 0;
		idx_t l_count = (l_block_idx == l_blocks.size() - 1 ? l_end : l_blocks[l_block_idx].first->count) - l_start;
		idx_t r_count = (r_block_idx == r_blocks.size() - 1 ? r_end : r_blocks[r_block_idx].first->count) - r_start;

		RowDataBlock write_sort, write_payl;
		write_sort.count = 0;
		write_payl.count = 0;
		write_sort.capacity = state.sorting.block_capacity;
		write_payl.capacity = state.payload.block_capacity;
		write_sort.block = buffer_manager.RegisterMemory(write_sort.capacity * state.sorting.entry_size, false);
		write_payl.block = buffer_manager.RegisterMemory(write_payl.capacity * state.payload.entry_size, false);
		auto write_sort_handle = buffer_manager.Pin(write_sort.block);
		auto write_payl_handle = buffer_manager.Pin(write_payl.block);
		auto write_sort_ptr = write_sort_handle->node->buffer;
		auto write_payl_ptr = write_payl_handle->node->buffer;

		while (l_offset != l_blocks.size() && r_offset != r_blocks.size()) {
			// allocate new blocks to write to
			if (write_sort.count == write_sort.capacity) {
				// append to result
				result.push_back(std::make_pair(move(write_sort), move(write_payl)));
				// initialize new blocks to write to
				write_sort = RowDataBlock();
				write_payl = RowDataBlock();
				write_sort.count = 0;
				write_payl.count = 0;
				write_sort.capacity = state.sorting.block_capacity;
				write_payl.capacity = state.payload.block_capacity;
				write_sort.block = buffer_manager.RegisterMemory(write_sort.capacity * state.sorting.entry_size, false);
				write_payl.block = buffer_manager.RegisterMemory(write_payl.capacity * state.payload.entry_size, false);
				write_sort_handle = buffer_manager.Pin(write_sort.block);
				write_payl_handle = buffer_manager.Pin(write_payl.block);
				write_sort_ptr = write_sort_handle->node->buffer;
				write_payl_ptr = write_payl_handle->node->buffer;
			}
			// load a new left or right block if needed
			if (l_offset == l_count) {
				l_block_idx++;
				if (l_block_idx != l_blocks.size()) {
					l_sort_handle = buffer_manager.Pin(l_blocks[l_block_idx].first->block);
					l_payl_handle = buffer_manager.Pin(l_blocks[l_block_idx].second->block);
					l_sort_ptr = l_sort_handle->node->buffer;
					r_sort_ptr = r_sort_handle->node->buffer;
					l_offset = 0;
					l_count = l_block_idx == l_blocks.size() - 1 ? l_end : l_blocks[l_block_idx].first->count;
				}
			} else if (r_offset == r_count) {
				r_block_idx++;
				if (r_block_idx != r_blocks.size()) {
					r_sort_handle = buffer_manager.Pin(r_blocks[r_block_idx].first->block);
					r_payl_handle = buffer_manager.Pin(r_blocks[r_block_idx].second->block);
					r_sort_ptr = r_sort_handle->node->buffer;
					r_payl_ptr = r_payl_handle->node->buffer;
					l_offset = 0;
					r_count = r_block_idx == r_blocks.size() - 1 ? r_end : r_blocks[r_block_idx].first->count;
				}
			}
			// append data and advance pointers
			if ((r_block_idx == r_blocks.size() && l_block_idx != l_blocks.size()) ||
			    compare_tuple(l_sort_ptr, r_sort_ptr, state) <= 0) {
				memcpy(write_sort_ptr, l_sort_ptr, state.sorting.entry_size);
				memcpy(write_payl_ptr, l_payl_ptr, state.payload.entry_size);
				l_sort_ptr += state.sorting.entry_size;
				l_payl_ptr += state.payload.entry_size;
				l_offset++;
			} else {
				memcpy(write_sort_ptr, r_sort_ptr, state.sorting.entry_size);
				memcpy(write_payl_ptr, r_payl_ptr, state.payload.entry_size);
				r_sort_ptr += state.sorting.entry_size;
				r_payl_ptr += state.payload.entry_size;
				r_offset++;
			}
			write_sort_ptr += state.sorting.entry_size;
			write_payl_ptr += state.payload.entry_size;
			write_sort.count++;
			write_payl.count++;
		}

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
	BufferManager &buffer_manager;

	vector<std::pair<RowDataBlock *, RowDataBlock *>> l_blocks;
	vector<std::pair<RowDataBlock *, RowDataBlock *>> r_blocks;
	vector<std::pair<RowDataBlock, RowDataBlock>> &result;

	idx_t l_start;
	idx_t r_start;
	idx_t l_end;
	idx_t r_end;
};

void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	// finalize: perform the actual sorting
	auto &sink = (OrderGlobalState &)*state;
	auto &scheduler = TaskScheduler::GetScheduler(context);

	// schedule sorting tasks for each block
	for (idx_t i = 0; i < sink.payload.blocks.size(); i++) {
		auto new_task = make_unique<PhysicalOrderSortTask>(pipeline, sink, BufferManager::GetBufferManager(context), i);
		scheduler.ScheduleTask(pipeline.token, move(new_task));
	}

	// schedule merging tasks until the data is merged
	idx_t n_per_thread = MaxValue(sink.sorting.count / scheduler.NumberOfThreads() + 1, sink.sorting.block_capacity);
	while (sink.payload.blocks.size() > 1) {
		while (sink.payload.blocks.size() > 1) {
			auto s_l = sink.sorting.blocks.erase(sink.sorting.blocks.begin());
			auto s_r = sink.sorting.blocks.erase(sink.sorting.blocks.begin());
			auto p_l = sink.payload.blocks.erase(sink.payload.blocks.begin());
			auto p_r = sink.payload.blocks.erase(sink.payload.blocks.begin());
			//			auto new_task = make_unique<PhysicalOrderMergeTask>(
			//			    pipeline, sink, BufferManager::GetBufferManager(context), *s_l, *s_r, *p_l, *p_r);
			//			scheduler.ScheduleTask(pipeline.token, move(new_task));
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
