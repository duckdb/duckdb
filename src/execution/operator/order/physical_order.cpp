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

struct ContinuousBlock {
public:
	ContinuousBlock(BufferManager &buffer_manager_) : buffer_manager(buffer_manager_), curr_block_idx(0) {
	}

	BufferManager &buffer_manager;
	idx_t sorting_entry_size;
	idx_t payload_entry_size;

	vector<shared_ptr<RowDataBlock>> sorting;
	vector<shared_ptr<RowDataBlock>> payload;
	idx_t start;
	idx_t end;

	data_ptr_t sorting_ptr;
	data_ptr_t payload_ptr;

	bool IsDone() const {
		return curr_block_idx >= sorting.size();
	}

	void PinBlock() {
		if (IsDone()) {
			return;
		}
		curr_entry_idx = curr_block_idx == 0 ? start : 0;
		curr_block_end = curr_block_idx == sorting.size() - 1 ? end : sorting[curr_block_idx]->count;
		sorting_handle = buffer_manager.Pin(sorting[curr_block_idx]->block);
		payload_handle = buffer_manager.Pin(payload[curr_block_idx]->block);
		sorting_ptr = sorting_handle->node->buffer + curr_entry_idx * sorting_entry_size;
		payload_ptr = payload_handle->node->buffer + curr_entry_idx * payload_entry_size;
	}

	void Advance() {
		curr_entry_idx++;
		if (curr_entry_idx < curr_block_end) {
			sorting_ptr += sorting_entry_size;
			payload_ptr += payload_entry_size;
		} else {
			curr_block_idx++;
			PinBlock();
		}
	}

	void FlushData(ContinuousBlock &result) {
		if (result.sorting.back()->count + (curr_block_end - curr_entry_idx) > result.sorting.back()->capacity) {
			// if it does not fit in the back, create new blocks to write to
			result.sorting.emplace_back(
			    make_shared<RowDataBlock>(buffer_manager, sorting[0]->capacity, sorting_entry_size));
			result.payload.emplace_back(
			    make_shared<RowDataBlock>(buffer_manager, payload[0]->capacity, payload_entry_size));
		}
		auto write_sort = result.sorting.back();
		auto write_payl = result.payload.back();
		auto write_sort_handle = buffer_manager.Pin(result.sorting.back()->block);
		auto write_payl_handle = buffer_manager.Pin(result.payload.back()->block);
		auto write_sort_ptr = write_sort_handle->node->buffer + write_sort->count * sorting_entry_size;
		auto write_payl_ptr = write_payl_handle->node->buffer + write_payl->count * payload_entry_size;

		for (; curr_entry_idx < curr_block_end; curr_entry_idx++) {
			memcpy(write_sort_ptr, sorting_ptr, sorting_entry_size);
			memcpy(write_payl_ptr, payload_ptr, payload_entry_size);
			write_sort_ptr += sorting_entry_size;
			write_payl_ptr += payload_entry_size;
			write_sort->count++;
			write_payl->count++;

			sorting_ptr += sorting_entry_size;
			payload_ptr += payload_entry_size;
		}
		curr_block_idx++;

		for (; curr_block_idx < sorting.size(); curr_block_idx++) {
			result.sorting.push_back(move(sorting[curr_block_idx]));
			result.payload.push_back(move(payload[curr_block_idx]));
		}
	}

private:
	idx_t curr_block_idx;
	idx_t curr_entry_idx;
	idx_t curr_block_end;

	unique_ptr<BufferHandle> sorting_handle;
	unique_ptr<BufferHandle> payload_handle;
};

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

	//! Ordered segments
	vector<unique_ptr<ContinuousBlock>> continuous;
	//! Intermediate results
	vector<unique_ptr<ContinuousBlock>> intermediate;
	//! Final sorted blocks
	vector<RowDataBlock> result;
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

	void Flush(RowChunk &sorting_, RowChunk &payload_) {
		// sink local sorting buffer into the given RowChunk
		for (auto next_chunk = sorting_buffer.Fetch(); next_chunk != nullptr; next_chunk = sorting_buffer.Fetch()) {
			// TODO: convert this to a bit representation that is sortable by memcmp?
			//  using 'orders' - this would reduce branch predictions in inner loop 'compare_tuple'
			sorting_.Build(next_chunk->size(), key_locations, nullmask_locations);
			for (idx_t i = 0; i < next_chunk->data.size(); i++) {
				sorting_.SerializeVector(next_chunk->data[i], next_chunk->size(), *sel_ptr, next_chunk->size(), i,
				                         key_locations, nullmask_locations);
			}
		}
		sorting_buffer.Reset();

		// sink local payload buffer into the given RowChunk
		for (auto next_chunk = payload_buffer.Fetch(); next_chunk != nullptr; next_chunk = payload_buffer.Fetch()) {
			payload_.Build(next_chunk->size(), key_locations, nullmask_locations);
			for (idx_t i = 0; i < next_chunk->data.size(); i++) {
				payload_.SerializeVector(next_chunk->data[i], next_chunk->size(), *sel_ptr, next_chunk->size(), i,
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
	PhysicalOrderSortTask(Pipeline &parent_, ClientContext &context_, OrderGlobalState &state_, idx_t block_idx_,
	                      ContinuousBlock &result_)
	    : parent(parent_), context(context_), buffer_manager(BufferManager::GetBufferManager(context_)), state(state_),
	      sort(state.sorting.blocks[block_idx_]), payl(state.payload.blocks[block_idx_]), result(result_) {
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
		auto sorting_ordered = ReOrder(state.sorting, sort, key_locations.get(), idxs.get());

		// now re-order payload data
		handle = buffer_manager.Pin(payl.block);
		dataptr = handle->node->buffer;
		for (idx_t i = 0; i < payl.count; i++) {
			key_locations[i] = dataptr;
			dataptr += state.payload.entry_size;
		}
		auto payload_ordered = ReOrder(state.payload, payl, key_locations.get(), idxs.get());

		result.start = 0;
		result.end = sorting_ordered->count;
		result.sorting.push_back(move(sorting_ordered));
		result.payload.push_back(move(payload_ordered));

		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
		// move on to merging step
		if (parent.total_tasks == parent.finished_tasks) {
			PhysicalOrder::ScheduleMergeTasks(parent, context, state);
		}
	}

private:
	void Sort(data_ptr_t key_locations[], idx_t idxs[]) {
		// create reference so it can be captured by the lambda function
		OrderGlobalState &state_ref = state;
		std::sort(idxs, idxs + sort.count, [key_locations, &state_ref](const idx_t &l_i, const idx_t &r_i) {
			return compare_tuple(key_locations[l_i], key_locations[r_i], state_ref) <= 0;
		});
	}

	shared_ptr<RowDataBlock> ReOrder(RowChunk &chunk, RowDataBlock &old_block, data_ptr_t key_locations[],
	                                 const idx_t idxs[]) {
		// initialize new block with same size as old block
		auto new_block = make_shared<RowDataBlock>(buffer_manager, old_block.capacity, chunk.entry_size);

		// pin the new block
		auto new_buffer_handle = buffer_manager.Pin(new_block->block);
		auto dataptr = new_buffer_handle->node->buffer;

		// copy data in correct order and unpin the new block when done
		for (idx_t i = 0; i < old_block.count; i++) {
			memcpy(dataptr + i * chunk.entry_size, key_locations[idxs[i]], chunk.entry_size);
		}
		new_block->count = old_block.count;

		return new_block;
	}

	Pipeline &parent;
	ClientContext &context;
	BufferManager &buffer_manager;
	OrderGlobalState &state;

	RowDataBlock &sort;
	RowDataBlock &payl;
	ContinuousBlock &result;
};

class PhysicalOrderMergeTask : public Task {
public:
	PhysicalOrderMergeTask(Pipeline &parent_, ClientContext &context_, OrderGlobalState &state_, ContinuousBlock &left_,
	                       ContinuousBlock &right_, ContinuousBlock &result_)
	    : parent(parent_), context(context_), buffer_manager(BufferManager::GetBufferManager(context_)), state(state_),
	      left(left_), right(right_), result(result_) {
	}

	void Execute() override {
		// initialize blocks to read from
		left.PinBlock();
		right.PinBlock();

		// initialize blocks to write to
		auto write_sort =
		    make_shared<RowDataBlock>(buffer_manager, state.sorting.block_capacity, state.sorting.entry_size);
		auto write_payl =
		    make_shared<RowDataBlock>(buffer_manager, state.payload.block_capacity, state.payload.entry_size);
		auto write_sort_handle = buffer_manager.Pin(write_sort->block);
		auto write_payl_handle = buffer_manager.Pin(write_payl->block);
		auto write_sort_ptr = write_sort_handle->node->buffer;
		auto write_payl_ptr = write_payl_handle->node->buffer;

		while (!left.IsDone() && !right.IsDone()) {
			if (write_sort->count == write_sort->capacity) {
				// append to result
				result.sorting.push_back((move(write_sort)));
				result.payload.push_back((move(write_payl)));
				// initialize new blocks to write to
				write_sort =
				    make_shared<RowDataBlock>(buffer_manager, state.sorting.block_capacity, state.sorting.entry_size);
				write_payl =
				    make_shared<RowDataBlock>(buffer_manager, state.payload.block_capacity, state.payload.entry_size);
				write_sort_handle = buffer_manager.Pin(write_sort->block);
				write_payl_handle = buffer_manager.Pin(write_payl->block);
				write_sort_ptr = write_sort_handle->node->buffer;
				write_payl_ptr = write_payl_handle->node->buffer;
			}
			if (compare_tuple(left.sorting_ptr, right.sorting_ptr, state) <= 0) {
				memcpy(write_sort_ptr, left.sorting_ptr, state.sorting.entry_size);
				memcpy(write_payl_ptr, left.payload_ptr, state.payload.entry_size);
				left.Advance();
			} else {
				memcpy(write_sort_ptr, right.sorting_ptr, state.sorting.entry_size);
				memcpy(write_payl_ptr, right.payload_ptr, state.payload.entry_size);
				right.Advance();
			}
			write_sort_ptr += state.sorting.entry_size;
			write_payl_ptr += state.payload.entry_size;
			write_sort->count++;
			write_payl->count++;
		}
		result.sorting.push_back((move(write_sort)));
		result.payload.push_back((move(write_payl)));

		// flush data of last block(s)
		if (!left.IsDone()) {
			left.FlushData(result);
		} else if (!right.IsDone()) {
			right.FlushData(result);
		}

		result.start = 0;
		result.end = result.sorting.back()->count;

		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
		if (parent.total_tasks == parent.finished_tasks) {
			PhysicalOrder::ScheduleMergeTasks(parent, context, state);
		}
	}

private:
	Pipeline &parent;
	ClientContext &context;
	BufferManager &buffer_manager;
	OrderGlobalState &state;

	ContinuousBlock &left;
	ContinuousBlock &right;
	ContinuousBlock &result;
};

void PhysicalOrder::ScheduleMergeTasks(Pipeline &pipeline, ClientContext &context, GlobalOperatorState &state) {
	auto &sink = (OrderGlobalState &)state;
	auto &bm = BufferManager::GetBufferManager(context);

	// cleanup - move intermediate to result
	for (auto &cb : sink.continuous) {
		for (idx_t i = 0; i < cb->sorting.size(); i++) {
			bm.UnregisterBlock(cb->sorting[i]->block->BlockId(), true);
			bm.UnregisterBlock(cb->payload[i]->block->BlockId(), true);
		}
	}
	sink.continuous.clear();
	for (auto &cb : sink.intermediate) {
		sink.continuous.push_back(move(cb));
	}
	sink.intermediate.clear();

	// finish pipeline if there is only one continuous block left
	if (sink.continuous.size() == 1) {
		// unregister sorting blocks and move payload blocks to result
		for (idx_t i = 0; i < sink.continuous[0]->sorting.size(); i++) {
			bm.UnregisterBlock(sink.continuous[0]->sorting[i]->block->BlockId(), true);
			sink.result.push_back(*sink.continuous[0]->payload[i]);
		}
		pipeline.Finish();
		return;
	}

	// if not, schedule tasks
	for (idx_t i = 0; i < sink.continuous.size() - 1; i += 2) {
		pipeline.total_tasks++;
		sink.intermediate.push_back(make_unique<ContinuousBlock>(bm));
		sink.intermediate.back()->sorting_entry_size = sink.sorting.entry_size;
		sink.intermediate.back()->payload_entry_size = sink.payload.entry_size;
		auto new_task = make_unique<PhysicalOrderMergeTask>(pipeline, context, sink, *sink.continuous[i],
		                                                    *sink.continuous[i + 1], *sink.intermediate.back());
		TaskScheduler::GetScheduler(context).ScheduleTask(pipeline.token, move(new_task));
	}

	// add last element if odd amount
	if (sink.continuous.size() % 2 == 1) {
		sink.intermediate.push_back(move(sink.continuous.back()));
		sink.continuous.pop_back();
	}
}

void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	this->sink_state = move(state);
	auto &sink = (OrderGlobalState &)*this->sink_state;

	// schedule sorting tasks for each block
	for (idx_t i = 0; i < sink.payload.blocks.size(); i++) {
		pipeline.total_tasks++;
		sink.intermediate.push_back(make_unique<ContinuousBlock>(BufferManager::GetBufferManager(context)));
		sink.intermediate.back()->sorting_entry_size = sink.sorting.entry_size;
		sink.intermediate.back()->payload_entry_size = sink.payload.entry_size;
		auto new_task = make_unique<PhysicalOrderSortTask>(pipeline, context, sink, i, *sink.intermediate.back());
		TaskScheduler::GetScheduler(context).ScheduleTask(pipeline.token, move(new_task));
	}
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

	if (state->current_block >= sink.result.size()) {
		for (const auto &b : sink.result) {
			BufferManager::GetBufferManager(context.client).UnregisterBlock(b.block->BlockId(), true);
		}
		state->finished = true;
		return;
	}

	sink.payload.DeserializeRowBlock(chunk, sink.result[state->current_block], state->position);

	state->position += STANDARD_VECTOR_SIZE;
	if (state->position >= sink.result[state->current_block].count) {
		state->current_block++;
		state->position = 0;
		BufferManager::GetBufferManager(context.client)
		    .UnregisterBlock(sink.result[state->current_block - 1].block->BlockId(), true);
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
