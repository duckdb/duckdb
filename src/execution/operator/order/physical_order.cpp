#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders)
    : PhysicalSink(PhysicalOperatorType::ORDER_BY, move(types)), orders(move(orders)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class OrderGlobalState : public GlobalOperatorState {
public:
	OrderGlobalState(PhysicalOrder &op, BufferManager &buffer_manager)
	    : op(op), buffer_manager(buffer_manager), row_chunk(buffer_manager), sorting_size(0), entry_size(0) {
	}
	PhysicalOrder &op;
	BufferManager &buffer_manager;

	//! The lock for updating the global order state
	mutex lock;

	//! To execute the expressions that are sorted
	ExpressionExecutor executor;
	vector<LogicalType> sorting_l_types;
	vector<PhysicalType> sorting_p_types;
	vector<PhysicalType> payload_p_types;

	//! Mappings from sorting index to payload index and vice versa
	std::unordered_map<idx_t, idx_t> s_to_p;
	std::unordered_map<idx_t, idx_t> p_to_s;
	std::unordered_map<idx_t, idx_t> p_to_p;

	//! Sorting columns in row format
	RowChunk row_chunk;
	idx_t sorting_nullmask_size;
	idx_t payload_nullmask_size;
	idx_t sorting_size;
	idx_t entry_size;
	idx_t block_capacity;

	//! Ordered segments
	vector<unique_ptr<ContinuousBlock>> continuous;
	//! Intermediate results
	vector<unique_ptr<ContinuousBlock>> intermediate;
};

class OrderLocalState : public LocalSinkState {
public:
	OrderLocalState(BufferManager &buffer_manager) : row_chunk(buffer_manager) {
	}

	//! Incoming data in row format
	RowChunk row_chunk;

	//! Allocate arrays for vector serialization
	const SelectionVector *sel_ptr = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE] {};
	data_ptr_t nullmask_locations[STANDARD_VECTOR_SIZE] {};

	//! Sorted incoming data (sorted each time a block is filled)
	vector<unique_ptr<ContinuousBlock>> continuous;
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
	auto state = make_unique<OrderGlobalState>(*this, BufferManager::GetBufferManager(context));
	for (idx_t i = 0; i < orders.size(); i++) {
		auto &expr = *orders[i].expression;
		state->executor.AddExpression(expr);
		state->sorting_l_types.push_back(expr.return_type);
		state->sorting_p_types.push_back(expr.return_type.InternalType());
		if (expr.type == ExpressionType::BOUND_REF) {
			auto &ref = (BoundReferenceExpression &)expr;
			state->s_to_p[i] = ref.index;
			state->p_to_s[ref.index] = i;
		}
	}
	for (idx_t i = 0; i < children[0]->types.size(); i++) {
		if (state->p_to_s.find(i) == state->p_to_s.end()) {
			// if the column is not already in sorting columns, add it to the payload
			state->p_to_p[state->payload_p_types.size()] = i;
			state->payload_p_types.push_back(children[0]->types[i].InternalType());
		}
	}
	state->sorting_nullmask_size = (state->sorting_p_types.size() + 7) / 8;
	state->payload_nullmask_size = (state->payload_p_types.size() + 7) / 8;
	state->entry_size += state->sorting_nullmask_size;
	for (auto &type : state->sorting_p_types) {
		state->sorting_size += GetTypeIdSize(type);
	}
	state->entry_size += state->sorting_size;
	state->entry_size += state->payload_nullmask_size;
	for (auto &type : state->payload_p_types) {
		state->entry_size += GetTypeIdSize(type);
	}
	idx_t vectors_per_block = (SORTING_BLOCK_SIZE / state->entry_size / STANDARD_VECTOR_SIZE);
	state->block_capacity = vectors_per_block * STANDARD_VECTOR_SIZE;
	return state;
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<OrderLocalState>(BufferManager::GetBufferManager(context.client));
}

struct ContinuousBlock {
public:
	ContinuousBlock(OrderGlobalState &state) : state(state), start(0), block_idx(0) {
	}
	OrderGlobalState &state;

	vector<RowDataBlock> blocks;
	idx_t start;
	idx_t end;

	unique_ptr<data_ptr_t[]> key_locations = nullptr;
	//! Used only for the initial merge after sorting in Sink
	unique_ptr<idx_t[]> offsets = nullptr; // FIXME: may need to share these offsets!

	data_ptr_t &DataPtr() {
		return key_locations[entry_idx];
	}

	void InitializeBlock() {
		if (block_idx >= blocks.size()) {
			return;
		}
		entry_idx = block_idx == 0 ? start : 0;
		block_end = block_idx == blocks.size() - 1 ? end : blocks[block_idx].count;
	}

	bool HasNext() {
		return entry_idx < block_end - 1 || block_idx < blocks.size() - 1;
	}

	void Advance() {
		if (entry_idx < block_end - 1) {
			entry_idx++;
		} else if (block_idx < blocks.size() - 1) {
			block_idx++;
			PinBlock();
		}
	}

	void PinBlock() {
		InitializeBlock();
		if (!HasNext()) {
			return;
		}
		handle = state.buffer_manager.Pin(blocks[block_idx].block);
		data_ptr_t dataptr = handle->node->buffer;
		if (!key_locations) {
			key_locations = unique_ptr<data_ptr_t[]>(new data_ptr_t[state.block_capacity]);
		}
		if (offsets) {
			for (idx_t i = entry_idx; i < end; i++) {
				key_locations[i] = dataptr + offsets[i];
			}
			offsets.release();
		} else {
			for (idx_t i = entry_idx; i < end; i++) {
				key_locations[i] = dataptr;
				dataptr += state.entry_size;
			}
		}
	}

	void FlushData(ContinuousBlock &target) {
		for (; block_idx < blocks.size(); block_idx++, InitializeBlock()) {
			if (offsets) {
				// this block has been sorted but not re-ordered
				PinBlock();
			} else if (entry_idx == 0 && block_end == blocks[block_idx].count) {
				// a full block can be appended
				target.blocks.push_back(blocks[block_idx]);
				continue;
			}
			// partial block must be appended
			if (target.blocks.empty() || target.blocks.back().count + (block_end - entry_idx) > state.block_capacity) {
				// if it does not fit in the back, create a new block to write to
				target.blocks.emplace_back(state.buffer_manager, state.block_capacity, state.entry_size);
			}
			auto &write_block = target.blocks.back();
			auto write_handle = state.buffer_manager.Pin(write_block.block);
			auto write_ptr = write_handle->node->buffer;

			// flush into last block
			for (; entry_idx < block_end; entry_idx++) {
				memcpy(write_ptr, DataPtr(), state.entry_size);
				write_ptr += state.entry_size;
				write_block.count++;
			}
		}
		target.end = target.blocks.back().count;
	}

private:
	idx_t block_idx;
	idx_t entry_idx;
	idx_t block_end;

	unique_ptr<BufferHandle> handle;
};

template <class TYPE>
static int8_t TemplatedCompareValue(data_ptr_t &l_val, data_ptr_t &r_val) {
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

static int32_t CompareValue(const data_ptr_t &l_nullmask, const data_ptr_t &r_nullmask, data_ptr_t &l_val,
                            data_ptr_t &r_val, const idx_t &sort_idx, OrderGlobalState &state) {
	bool left_null = *l_nullmask & (1 << sort_idx);
	bool right_null = *r_nullmask & (1 << sort_idx);

	if (left_null && right_null) {
		return 0;
	} else if (right_null) {
		return state.op.orders[sort_idx].null_order == OrderByNullType::NULLS_FIRST ? 1 : -1;
	} else if (left_null) {
		return state.op.orders[sort_idx].null_order == OrderByNullType::NULLS_FIRST ? -1 : 1;
	}

	switch (state.sorting_p_types[sort_idx]) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedCompareValue<int8_t>(l_val, r_val);
	case PhysicalType::INT16:
		return TemplatedCompareValue<int16_t>(l_val, r_val);
	case PhysicalType::INT32:
		return TemplatedCompareValue<int32_t>(l_val, r_val);
	case PhysicalType::INT64:
		return TemplatedCompareValue<int64_t>(l_val, r_val);
	case PhysicalType::UINT8:
		return TemplatedCompareValue<uint8_t>(l_val, r_val);
	case PhysicalType::UINT16:
		return TemplatedCompareValue<uint16_t>(l_val, r_val);
	case PhysicalType::UINT32:
		return TemplatedCompareValue<uint32_t>(l_val, r_val);
	case PhysicalType::UINT64:
		return TemplatedCompareValue<uint64_t>(l_val, r_val);
	case PhysicalType::INT128:
		return TemplatedCompareValue<hugeint_t>(l_val, r_val);
	case PhysicalType::FLOAT:
		return TemplatedCompareValue<float>(l_val, r_val);
	case PhysicalType::DOUBLE:
		return TemplatedCompareValue<double>(l_val, r_val);
	case PhysicalType::VARCHAR:
		return TemplatedCompareValue<string_t>(l_val, r_val);
	case PhysicalType::INTERVAL:
		return TemplatedCompareValue<interval_t>(l_val, r_val);
	default:
		throw NotImplementedException("Type for comparison");
	}
}

static int CompareTuple(const data_ptr_t &l_start, const data_ptr_t &r_start, OrderGlobalState &state) {
	auto l_val = l_start + state.sorting_nullmask_size;
	auto r_val = r_start + state.sorting_nullmask_size;
	for (idx_t i = 0; i < state.op.orders.size(); i++) {
		auto comp_res = CompareValue(l_start, r_start, l_val, r_val, i, state);
		if (comp_res == 0) {
			idx_t val_size = GetTypeIdSize(state.sorting_p_types[i]);
			l_val += val_size;
			r_val += val_size;
			continue;
		}
		return comp_res < 0 ? (state.op.orders[i].type == OrderType::ASCENDING ? -1 : 1)
		                    : (state.op.orders[i].type == OrderType::ASCENDING ? 1 : -1);
	}
	return 0;
}

void Sort(ContinuousBlock &cb, OrderGlobalState &state) {
	D_ASSERT(cb.blocks.size() == 1);
	auto &block = cb.blocks[0];
	cb.end = block.count;

	auto handle = state.row_chunk.buffer_manager.Pin(block.block);
	data_ptr_t dataptr = handle->node->buffer;

	// fetch a batch of pointers to entries in the blocks, and initialize idxs
	cb.key_locations = unique_ptr<data_ptr_t[]>(new data_ptr_t[block.count]);
	for (idx_t i = 0; i < block.count; i++) {
		cb.key_locations[i] = dataptr;
		dataptr += state.entry_size;
	}

	std::sort(cb.key_locations.get(), cb.key_locations.get() + block.count,
	          [&state](const data_ptr_t &l, const data_ptr_t &r) { return CompareTuple(l, r, state) <= 0; });

	// convert sorted pointers to offsets
	cb.offsets = unique_ptr<idx_t[]>(new idx_t[block.count]);
	for (idx_t i = 0; i < block.count; i++) {
		cb.offsets[i] = cb.key_locations[i] - handle->node->buffer;
	}
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p,
                         DataChunk &input) {
	auto &gstate = (OrderGlobalState &)state;
	auto &lstate = (OrderLocalState &)lstate_p;

	if (lstate.row_chunk.blocks.empty()) {
		// init using global state
		lstate.row_chunk.entry_size = gstate.entry_size;
		lstate.row_chunk.block_capacity = gstate.block_capacity;
	}

	// obtain sorting columns
	DataChunk sort;
	sort.Initialize(gstate.sorting_l_types);
	gstate.executor.Execute(input, sort);

	// build the block
	lstate.row_chunk.Build(input.size(), lstate.nullmask_locations);
	for (idx_t i = 0; i < sort.size(); i++) {
		// initialize nullmasks to 0
		memset(lstate.nullmask_locations[i], 0, gstate.sorting_nullmask_size);
		lstate.key_locations[i] = lstate.nullmask_locations[i] + gstate.sorting_nullmask_size;
	}
	// serialize sorting columns to row-wise format
	for (idx_t i = 0; i < sort.data.size(); i++) {
		lstate.row_chunk.SerializeVector(sort.data[i], sort.size(), *lstate.sel_ptr, sort.size(), i,
		                                 lstate.key_locations, lstate.nullmask_locations);
	}
	// move key- and nullmask locations for the payload columns
	for (idx_t i = 0; i < input.size(); i++) {
		lstate.nullmask_locations[i] = lstate.key_locations[i];
		// initialize nullmasks to 0
		memset(lstate.nullmask_locations[i], 0, gstate.payload_nullmask_size);
		lstate.key_locations[i] = lstate.nullmask_locations[i] + gstate.payload_nullmask_size;
	}
	// serialize payload columns to row-wise format
	for (idx_t i = 0; i < input.data.size(); i++) {
		if (gstate.p_to_s.find(i) != gstate.p_to_s.end()) {
			// this column is already serialized as a sorting column
			continue;
		}
		lstate.row_chunk.SerializeVector(input.data[i], input.size(), *lstate.sel_ptr, input.size(), gstate.p_to_p[i],
		                                 lstate.key_locations, lstate.nullmask_locations);
	}
	// sort the block if it is full
	if (!lstate.row_chunk.blocks.empty() && lstate.row_chunk.blocks.back().count == gstate.block_capacity) {
		lstate.continuous.push_back(make_unique<ContinuousBlock>(gstate));
		lstate.continuous.back()->blocks.push_back(lstate.row_chunk.blocks.back());
		Sort(*lstate.continuous.back(), gstate);
	}
}

void PhysicalOrder::Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p) {
	auto &gstate = (OrderGlobalState &)state;
	auto &lstate = (OrderLocalState &)lstate_p;

	if (lstate.row_chunk.blocks.empty()) {
		return;
	}

	ContinuousBlock *unsorted_block = nullptr;
	{
		// append to global state. this initializes gstate RowChunks if empty
		lock_guard<mutex> glock(gstate.lock);
		gstate.row_chunk.Append(lstate.row_chunk);
		for (auto &c : lstate.continuous) {
			gstate.intermediate.push_back(move(c));
		}
		if (lstate.row_chunk.blocks.back().count != gstate.block_capacity) {
			// last block not full - create ContinuousBlock and set the pointer
			gstate.intermediate.push_back(make_unique<ContinuousBlock>(gstate));
			unsorted_block = gstate.intermediate.back().get();
		}
	}

	if (unsorted_block) {
		// there was a unsorted block - push it into the ContinuousBlock and sort it
		unsorted_block->blocks.push_back(lstate.row_chunk.blocks.back());
		Sort(*unsorted_block, gstate);
	}
}

class PhysicalOrderMergeTask : public Task {
public:
	PhysicalOrderMergeTask(Pipeline &parent, ClientContext &context, OrderGlobalState &state, ContinuousBlock &left,
	                       ContinuousBlock &right, ContinuousBlock &result)
	    : parent(parent), context(context), buffer_manager(BufferManager::GetBufferManager(context)), state(state),
	      left(left), right(right), result(result) {
	}

	void Execute() override {
		// initialize blocks to read from
		left.PinBlock();
		right.PinBlock();

		// initialize blocks to write to
		RowDataBlock write_block(buffer_manager, state.block_capacity, state.entry_size);
		auto write_handle = buffer_manager.Pin(write_block.block);
		auto write_ptr = write_handle->node->buffer;

		while (left.HasNext() && right.HasNext()) {
			if (write_block.count == write_block.capacity) {
				// append to result
				result.blocks.push_back(write_block);
				// initialize new blocks to write to
				write_block = RowDataBlock(buffer_manager, state.block_capacity, state.entry_size);
				write_handle = buffer_manager.Pin(write_block.block);
				write_ptr = write_handle->node->buffer;
			}
			if (CompareTuple(left.DataPtr(), right.DataPtr(), state) <= 0) {
				memcpy(write_ptr, left.DataPtr(), state.entry_size);
				left.Advance();
			} else {
				memcpy(write_ptr, right.DataPtr(), state.entry_size);
				right.Advance();
			}
			write_ptr += state.entry_size;
			write_block.count++;
		}
		result.blocks.push_back(write_block);

		// flush data of last block(s)
		if (left.HasNext()) {
			left.FlushData(result);
		} else {
			right.FlushData(result);
		}

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
		for (idx_t i = 0; i < cb->blocks.size(); i++) {
			bm.UnregisterBlock(cb->blocks[i].block->BlockId(), true);
		}
	}
	sink.continuous.clear();
	for (auto &cb : sink.intermediate) {
		sink.continuous.push_back(move(cb));
	}
	sink.intermediate.clear();

	// finish pipeline if there is only one continuous block left
	if (sink.continuous.size() == 1) {
		//		pipeline.Finish(); // TODO: is this needed?
		return;
	}

	// if not, schedule tasks
	for (idx_t i = 0; i < sink.continuous.size() - 1; i += 2) {
		pipeline.total_tasks++;
		sink.intermediate.push_back(make_unique<ContinuousBlock>(sink));
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

	if (sink.intermediate.capacity() == 1) {
		// special case: only one block arrived, it was sorted but not re-ordered
		auto single_block = move(sink.intermediate[0]);
		sink.intermediate[0] = make_unique<ContinuousBlock>(sink);
		single_block->FlushData(*sink.intermediate[0]);
	}

	ScheduleMergeTasks(pipeline, context, sink);
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

void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_p);
	auto &sink = (OrderGlobalState &)*this->sink_state;

	D_ASSERT(sink.continuous.size() == 1);

	if (state->current_block >= sink.continuous[0]->blocks.size()) {
		state->finished = true;
		return;
	}

	auto &block = sink.continuous[0]->blocks[state->current_block];
	auto handle = sink.buffer_manager.Pin(block.block);
	auto dataptr = handle->node->buffer + state->position * sink.entry_size;

	data_ptr_t nullmask_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];

	// fetch the next vector of entries from the blocks
	idx_t next = MinValue<idx_t>(STANDARD_VECTOR_SIZE, block.count - state->position);
	for (idx_t i = 0; i < next; i++) {
		nullmask_locations[i] = dataptr;
		key_locations[i] = dataptr + sink.sorting_nullmask_size;
		dataptr += sink.entry_size;
	}
	chunk.SetCardinality(next);

	// deserialize sorting columns (if needed)
	for (idx_t sort_idx = 0; sort_idx < sink.sorting_p_types.size(); sort_idx++) {
		if (sink.s_to_p.find(sort_idx) == sink.s_to_p.end()) {
			// sorting column does not need to be output
			idx_t size = GetTypeIdSize(sink.sorting_p_types[sort_idx]);
			for (idx_t i = 0; i < next; i++) {
				key_locations[i] += size;
			}
		} else {
			// sorting column needs to be output
			sink.row_chunk.DeserializeIntoVector(chunk.data[sink.s_to_p[sort_idx]], next, sort_idx, key_locations,
			                                     nullmask_locations);
		}
	}

	// move pointers to payload
	for (idx_t i = 0; i < next; i++) {
		nullmask_locations[i] = key_locations[i];
		key_locations[i] += sink.payload_nullmask_size;
	}

	// deserialize payload columns
	for (idx_t payl_idx = 0; payl_idx < sink.payload_p_types.size(); payl_idx++) {
		sink.row_chunk.DeserializeIntoVector(chunk.data[sink.p_to_p[payl_idx]], next, payl_idx, key_locations,
		                                     nullmask_locations);
	}
	chunk.Verify();

	state->position += STANDARD_VECTOR_SIZE;
	if (state->position >= block.count) {
		state->current_block++;
		state->position = 0;
		BufferManager::GetBufferManager(context.client).UnregisterBlock(block.block->BlockId(), true);
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
