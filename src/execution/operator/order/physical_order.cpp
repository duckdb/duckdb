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
struct SortingState {
	SortingState(const vector<PhysicalType> &sorting_p_types, const vector<idx_t> &sorting_p_sizes,
	             const vector<OrderByNullType> &null_orders, const vector<OrderType> &order_types,
	             const idx_t &sorting_nullmask_size, const idx_t &payload_nullmask_size, const idx_t &sorting_size,
	             const idx_t &entry_size, const idx_t &block_capacity, const vector<int8_t> &is_asc,
	             const vector<int8_t> &nulls_first)
	    : SORTING_P_TYPES(sorting_p_types), SORTING_P_SIZES(sorting_p_sizes), NULL_ORDERS(null_orders),
	      ORDER_TYPES(order_types), SORTING_NULLMASK_SIZE(sorting_nullmask_size),
	      PAYLOAD_NULLMASK_SIZE(payload_nullmask_size), SORTING_SIZE(sorting_size), ENTRY_SIZE(entry_size),
	      BLOCK_CAPACITY(block_capacity), IS_ASC(is_asc), NULLS_FIRST(nulls_first) {
	}
	const vector<PhysicalType> SORTING_P_TYPES;
	const vector<idx_t> SORTING_P_SIZES;
	const vector<OrderByNullType> NULL_ORDERS;
	const vector<OrderType> ORDER_TYPES;

	const idx_t SORTING_NULLMASK_SIZE;
	const idx_t PAYLOAD_NULLMASK_SIZE;
	const idx_t SORTING_SIZE;
	const idx_t ENTRY_SIZE;
	const idx_t BLOCK_CAPACITY;

	const uint8_t NULL_BITS[8] = {1, 2, 4, 8, 16, 32, 64, 128};
	const vector<int8_t> IS_ASC;
	const vector<int8_t> NULLS_FIRST;
};

class OrderGlobalState : public GlobalOperatorState {
public:
	OrderGlobalState(PhysicalOrder &op, BufferManager &buffer_manager)
	    : op(op), buffer_manager(buffer_manager), row_chunk(buffer_manager), sorting_size(0), entry_size(0),
	      merge_path(false) {
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
	vector<vector<unique_ptr<ContinuousBlock>>> intermediate;

	void InitializeSortingState(const vector<idx_t> &sorting_p_sizes, const vector<OrderByNullType> &null_orders,
	                            const vector<OrderType> &order_types, const vector<int8_t> &is_asc,
	                            const vector<int8_t> &nulls_first) {
		sorting_state = make_unique<SortingState>(sorting_p_types, sorting_p_sizes, null_orders, order_types,
		                                          sorting_nullmask_size, payload_nullmask_size, sorting_size,
		                                          entry_size, block_capacity, is_asc, nulls_first);
	}

	//! Bunch of const for speed during sorting
	unique_ptr<SortingState> sorting_state;
	//! Whether a merge path is currently going on
	bool merge_path;
	//! Index of the current 'left' block for MergePath
	idx_t mp_block_idx;
	//! Slices of the left and right blocks for the current Merge Path
	vector<unique_ptr<ContinuousBlock>> left_slices;
	vector<unique_ptr<ContinuousBlock>> right_slices;
	//! Computed intersections for Merge Path
	vector<std::pair<idx_t, idx_t>> left_intersections;
	vector<std::pair<idx_t, idx_t>> right_intersections;
};

class OrderLocalState : public LocalSinkState {
public:
	explicit OrderLocalState(BufferManager &buffer_manager) : row_chunk(buffer_manager) {
	}

	//! Incoming data in row format
	RowChunk row_chunk;

	//! Allocate arrays for vector serialization
	const SelectionVector *sel_ptr = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t nullmask_locations[STANDARD_VECTOR_SIZE];

	//! Sorted incoming data (sorted each time a block is filled)
	vector<unique_ptr<ContinuousBlock>> continuous;
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
	auto state = make_unique<OrderGlobalState>(*this, BufferManager::GetBufferManager(context));
	vector<idx_t> sorting_p_sizes;
	vector<OrderByNullType> null_orders;
	vector<OrderType> order_types;
	vector<int8_t> is_asc;
	vector<int8_t> nulls_first;
	for (idx_t i = 0; i < orders.size(); i++) {
		null_orders.push_back(orders[i].null_order);
		order_types.push_back(orders[i].type);
		is_asc.push_back(orders[i].type == OrderType::ASCENDING ? 1 : -1);
		nulls_first.push_back(orders[i].null_order == OrderByNullType::NULLS_FIRST ? 1 : -1);
		auto &expr = *orders[i].expression;
		state->executor.AddExpression(expr);
		state->sorting_l_types.push_back(expr.return_type);
		state->sorting_p_types.push_back(expr.return_type.InternalType());
		sorting_p_sizes.push_back(GetTypeIdSize(expr.return_type.InternalType()));
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
	state->InitializeSortingState(sorting_p_sizes, null_orders, order_types, is_asc, nulls_first);
	return state;
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<OrderLocalState>(BufferManager::GetBufferManager(context.client));
}

struct ContinuousBlock {
public:
	explicit ContinuousBlock(OrderGlobalState &state) : state(state), start(0), block_idx(0) {
	}
	OrderGlobalState &state;

	vector<RowDataBlock> blocks;
	idx_t start;
	idx_t end;

	//! Used only for the initial merge after sorting in Sink/Combine
	shared_ptr<idx_t[]> offsets = nullptr;

	idx_t Count() {
		idx_t count = 0;
		for (idx_t i = 0; i < blocks.size(); i++) {
			count += blocks[i].count;
		}
		count -= start;
		count -= blocks.back().count - end;
		return count;
	}

	data_ptr_t &DataPtr() {
		return dataptr;
	}

	bool Done() {
		return block_idx >= blocks.size();
	}

	void Advance() {
		if (entry_idx < block_end - 1) {
            entry_idx++;
            if (offsets) {
                dataptr = handle->node->buffer + offsets[entry_idx];
            } else {
                dataptr += state.entry_size;
            }
		} else if (block_idx < blocks.size()) {
			block_idx++;
			PinBlock();
		}
	}

	void PinBlock() {
		if (Done()) {
			return;
		}
        entry_idx = block_idx == 0 ? start : 0;
        block_end = block_idx == blocks.size() - 1 ? end : blocks[block_idx].count;
		handle = state.buffer_manager.Pin(blocks[block_idx].block);
		if (offsets) {
			dataptr = handle->node->buffer + offsets[entry_idx];
		} else {
			dataptr = handle->node->buffer + entry_idx * state.entry_size;
		}
	}

	void FlushData(ContinuousBlock &target) {
		RowDataBlock *write_block = nullptr;
		unique_ptr<BufferHandle> write_handle;
		data_ptr_t write_ptr;

		if (!target.blocks.empty()) {
			write_block = &target.blocks.back();
            write_handle = state.buffer_manager.Pin(write_block->block);
            write_ptr = write_handle->node->buffer + write_block->count * state.entry_size;
		}

        // flush data of last block(s)
        while (!Done()) {
            if (!write_block || write_block->count == write_block->capacity) {
                // initialize new blocks to write to
				target.blocks.emplace_back(state.buffer_manager, state.block_capacity, state.entry_size);
				write_block = &target.blocks.back();
                write_handle = state.buffer_manager.Pin(write_block->block);
                write_ptr = write_handle->node->buffer;
            }
            memcpy(write_ptr, DataPtr(), state.entry_size);
			Advance();
            write_ptr += state.entry_size;
            write_block->count++;
        }
        target.end = target.blocks.back().count;
	}

	std::pair<idx_t, idx_t> GlobalToLocalIndex(idx_t global_idx) {
		idx_t local_block_idx;
		for (local_block_idx = 0; local_block_idx < blocks.size(); local_block_idx++) {
			if (global_idx < blocks[local_block_idx].count) {
				break;
			}
			global_idx -= blocks[local_block_idx].count;
		}
		return std::make_pair(local_block_idx, global_idx);
	}

	unique_ptr<ContinuousBlock> Slice(const idx_t &starting_block, const idx_t &starting_entry,
	                                  const idx_t &ending_block, const idx_t &ending_entry) {
		auto result = make_unique<ContinuousBlock>(state);
		result->start = starting_entry;
		for (idx_t i = starting_block; i <= ending_block; i++) {
			result->blocks.push_back(blocks[i]);
		}
		result->end = ending_entry;
		result->offsets = offsets;
		return result;
	}

private:
	idx_t block_idx;
	idx_t entry_idx;
	idx_t block_end;

	unique_ptr<BufferHandle> handle;
	data_ptr_t dataptr;
};

template <class TYPE>
static int8_t TemplatedCompareValue(const data_ptr_t &l_val, const data_ptr_t &r_val) {
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

static int32_t CompareValue(const data_ptr_t &l_nullmask, const data_ptr_t &r_nullmask, const data_ptr_t &l_val,
                            const data_ptr_t &r_val, const idx_t &sort_idx, const SortingState &state) {
	auto byte_offset = sort_idx / 8;
	auto offset_in_byte = sort_idx % 8;
	auto left_null = *(l_nullmask + byte_offset) & state.NULL_BITS[offset_in_byte];
	auto right_null = *(r_nullmask + byte_offset) & state.NULL_BITS[offset_in_byte];

	if (left_null && right_null) {
		return 0;
	} else if (right_null) {
		return state.NULLS_FIRST[sort_idx];
	} else if (left_null) {
		return -state.NULLS_FIRST[sort_idx];
	}

	switch (state.SORTING_P_TYPES[sort_idx]) {
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

static int CompareTuple(const data_ptr_t &l_start, const data_ptr_t &r_start, const SortingState &state) {
	auto l_val = l_start + state.SORTING_NULLMASK_SIZE;
	auto r_val = r_start + state.SORTING_NULLMASK_SIZE;
	for (idx_t i = 0; i < state.SORTING_P_TYPES.size(); i++) {
		auto comp_res = CompareValue(l_start, r_start, l_val, r_val, i, state);
		if (comp_res == 0) {
			l_val += state.SORTING_P_SIZES[i];
			r_val += state.SORTING_P_SIZES[i];
			continue;
		}
		return comp_res * state.IS_ASC[i];
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
	auto key_locations = unique_ptr<data_ptr_t[]>(new data_ptr_t[block.count]);
	for (idx_t i = 0; i < block.count; i++) {
		key_locations[i] = dataptr;
		dataptr += state.entry_size;
	}

	data_ptr_t *start = key_locations.get();
	data_ptr_t *end = start + block.count;
	const auto &sorting_state = *state.sorting_state;
	std::sort(start, end, [&sorting_state](const data_ptr_t &l, const data_ptr_t &r) {
		return CompareTuple(l, r, sorting_state) <= 0;
	});

	// convert sorted pointers to offsets
	cb.offsets = shared_ptr<idx_t[]>(new idx_t[block.count]);
	for (idx_t i = 0; i < block.count; i++) {
		cb.offsets[i] = key_locations[i] - handle->node->buffer;
	}
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p,
                         DataChunk &input) {
	auto &gstate = (OrderGlobalState &)state;
	auto &lstate = (OrderLocalState &)lstate_p;
	const auto &sorting_state = *gstate.sorting_state;

	if (lstate.row_chunk.blocks.empty()) {
		// init using global state
		lstate.row_chunk.entry_size = sorting_state.ENTRY_SIZE;
		lstate.row_chunk.block_capacity = sorting_state.BLOCK_CAPACITY;
	}

	// obtain sorting columns
	DataChunk sort;
	sort.Initialize(gstate.sorting_l_types);
	gstate.executor.Execute(input, sort);

	// build the block
	lstate.row_chunk.Build(input.size(), lstate.nullmask_locations);
	for (idx_t i = 0; i < sort.size(); i++) {
		// initialize nullmasks to 0
		memset(lstate.nullmask_locations[i], 0, sorting_state.SORTING_NULLMASK_SIZE);
		lstate.key_locations[i] = lstate.nullmask_locations[i] + sorting_state.SORTING_NULLMASK_SIZE;
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
		memset(lstate.nullmask_locations[i], 0, sorting_state.PAYLOAD_NULLMASK_SIZE);
		lstate.key_locations[i] = lstate.nullmask_locations[i] + sorting_state.PAYLOAD_NULLMASK_SIZE;
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
			gstate.intermediate.emplace_back();
			gstate.intermediate.back().push_back(move(c));
		}
		if (lstate.row_chunk.blocks.back().count != gstate.block_capacity) {
			// last block not full - create ContinuousBlock and set the pointer
			gstate.intermediate.emplace_back();
			gstate.intermediate.back().push_back(make_unique<ContinuousBlock>(gstate));
			unsorted_block = gstate.intermediate.back().back().get();
            unsorted_block->blocks.push_back(lstate.row_chunk.blocks.back());
		}
	}

	if (unsorted_block) {
		// there was a unsorted block - push it into the ContinuousBlock and sort it
		Sort(*unsorted_block, gstate);
	}
}

class PhysicalOrderMergePathTask : public Task {
public:
	PhysicalOrderMergePathTask(Pipeline &parent, ClientContext &context, OrderGlobalState &state, idx_t sum,
	                           ContinuousBlock &left, ContinuousBlock &right, idx_t result_idx)
	    : parent(parent), context(context), buffer_manager(BufferManager::GetBufferManager(context)), state(state),
	      sum(sum), left(left), right(right), result_idx(result_idx) {
	}

	void Execute() override {
		const auto &sorting_state = *state.sorting_state;
		const idx_t l_count = left.Count();
		const idx_t r_count = right.Count();

		// determine bounds of the intersection
		idx_t l_lower = r_count > sum ? 0 : sum - r_count;
		idx_t r_upper = MinValue(sum, r_count);

		RowDataBlock *l_block = nullptr;
		RowDataBlock *r_block = nullptr;
		unique_ptr<BufferHandle> l_block_handle;
		unique_ptr<BufferHandle> r_block_handle;
		data_ptr_t l_dataptr;
		data_ptr_t r_dataptr;
		std::pair<idx_t, idx_t> l_block_entry = std::make_pair(0, 0);
		std::pair<idx_t, idx_t> r_block_entry = std::make_pair(0, 0);

		// binary search
		idx_t search_start = 0;
		idx_t search_end = MinValue(sum, l_count) - l_lower;
		while (search_start <= search_end) {
			// determine the entries to compare from each block
			idx_t middle = (search_start + search_end) / 2;
			l_block_entry = left.GlobalToLocalIndex(l_lower + middle);
			r_block_entry = right.GlobalToLocalIndex(r_upper - middle);

			// pin blocks if needed
			if (!l_block || l_block->block->BlockId() != left.blocks[l_block_entry.first].block->BlockId()) {
				l_block = &left.blocks[l_block_entry.first];
				l_block_handle = buffer_manager.Pin(l_block->block);
			}
			if (!r_block || r_block->block->BlockId() != right.blocks[r_block_entry.first].block->BlockId()) {
				r_block = &right.blocks[r_block_entry.first];
				r_block_handle = buffer_manager.Pin(r_block->block);
			}

			// obtain left and right data pointers
			if (left.offsets) {
				l_dataptr = l_block_handle->node->buffer + left.offsets[l_block_entry.second];
			} else {
                l_dataptr = l_block_handle->node->buffer + l_block_entry.second * sorting_state.ENTRY_SIZE;
			}
            if (right.offsets) {
                r_dataptr = r_block_handle->node->buffer + right.offsets[r_block_entry.second];
            } else {
                r_dataptr = r_block_handle->node->buffer + r_block_entry.second * sorting_state.ENTRY_SIZE;
            }

            // compare tuples and update search boundaries
			auto comp_res = CompareTuple(l_dataptr, r_dataptr, sorting_state);
			if (comp_res == 0) {
				// left and right are equal - it's always ok to merge here
				break;
			} else if (comp_res < 0) {
				// left side is smaller
                search_start = middle + 1;
			} else {
				// right side is smaller
                search_end = middle - 1;
			}
		}
		// set values in the result
		state.left_intersections[result_idx] = l_block_entry;
		state.right_intersections[result_idx] = r_block_entry;

		// update global state
		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
		if (parent.total_tasks == parent.finished_tasks) {
			PhysicalOrder::ScheduleMergePathTasks(parent, context, state);
		}
	}

private:
	Pipeline &parent;
	ClientContext &context;
	BufferManager &buffer_manager;
	OrderGlobalState &state;

	idx_t sum;
	ContinuousBlock &left;
	ContinuousBlock &right;
	idx_t result_idx;
};

class PhysicalOrderMergeTask : public Task {
public:
	PhysicalOrderMergeTask(Pipeline &parent, ClientContext &context, OrderGlobalState &state, ContinuousBlock &left,
	                       ContinuousBlock &right, ContinuousBlock &result)
	    : parent(parent), context(context), buffer_manager(BufferManager::GetBufferManager(context)), state(state),
	      left(left), right(right), result(result) {
	}

	void Execute() override {
		const auto &sorting_state = *state.sorting_state;

		// initialize blocks to read from
		left.PinBlock();
		right.PinBlock();

		// initialize blocks to write to
		result.blocks.emplace_back(buffer_manager, sorting_state.BLOCK_CAPACITY, sorting_state.ENTRY_SIZE);
		auto write_block = &result.blocks.back();
		auto write_handle = buffer_manager.Pin(write_block->block);
		auto write_ptr = write_handle->node->buffer;

		while (!left.Done() && !right.Done()) {
			if (write_block->count == write_block->capacity) {
				// initialize new blocks to write to
                result.blocks.emplace_back(buffer_manager, sorting_state.BLOCK_CAPACITY, sorting_state.ENTRY_SIZE);
                write_block = &result.blocks.back();
				write_handle = buffer_manager.Pin(write_block->block);
				write_ptr = write_handle->node->buffer;
			}
			if (CompareTuple(left.DataPtr(), right.DataPtr(), sorting_state) <= 0) {
				memcpy(write_ptr, left.DataPtr(), sorting_state.ENTRY_SIZE);
				left.Advance();
			} else {
				memcpy(write_ptr, right.DataPtr(), sorting_state.ENTRY_SIZE);
				right.Advance();
			}
			write_ptr += sorting_state.ENTRY_SIZE;
			write_block->count++;
		}

		if (left.Done()) {
			right.FlushData(result);
		} else {
			left.FlushData(result);
		}

		D_ASSERT(result.Count() == left.Count() + right.Count());

		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
		if (parent.total_tasks == parent.finished_tasks) {
			if (state.merge_path) {
				PhysicalOrder::ScheduleMergePathTasks(parent, context, state);
			} else {
				PhysicalOrder::ScheduleMergeTasks(parent, context, state);
			}
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

void PhysicalOrder::ScheduleMergePathTasks(Pipeline &pipeline, ClientContext &context, GlobalOperatorState &state) {
	auto &sink = (OrderGlobalState &)state;
	D_ASSERT(sink.left_intersections.size() == sink.right_intersections.size());

	if (sink.mp_block_idx >= sink.continuous.size()) {
		// this iteration of merge path is complete
		sink.merge_path = false;
		PhysicalOrder::ScheduleMergeTasks(pipeline, context, state);
		return;
	}
	auto &ts = TaskScheduler::GetScheduler(context);

	// these two blocks are currently being merged
	auto &merge_path_left = *sink.continuous[sink.mp_block_idx];
	auto &merge_path_right = *sink.continuous[sink.mp_block_idx + 1];

	if (sink.left_intersections.empty()) {
		// there are no intersections, do some cleanup
		sink.left_slices.clear();
		sink.right_slices.clear();
		// schedule n - 1 tasks to compute merge path intersections
		idx_t n_threads = ts.NumberOfThreads();
		idx_t tuples_per_thread = (merge_path_left.Count() + merge_path_right.Count()) / n_threads;
		for (idx_t i = 1; i < n_threads; i++) {
			sink.left_intersections.emplace_back();
			sink.right_intersections.emplace_back();
			auto new_task =
			    make_unique<PhysicalOrderMergePathTask>(pipeline, context, sink, i * tuples_per_thread, merge_path_left,
			                                            merge_path_right, sink.left_intersections.size() - 1);
			pipeline.total_tasks++;
			ts.ScheduleTask(pipeline.token, move(new_task));
		}
		// add final "intersection": the end of each block
		sink.left_intersections.emplace_back(merge_path_left.blocks.size() - 1, merge_path_left.end);
		sink.right_intersections.emplace_back(merge_path_right.blocks.size() - 1, merge_path_right.end);
		return;
	}

	// there are intersections, schedule merge tasks
	sink.intermediate.emplace_back();
	auto l_prev = std::make_pair(0, 0);
	auto r_prev = std::make_pair(0, 0);
	for (idx_t i = 0; i < sink.left_intersections.size(); i++) {
		// slice the block along the intersections
		auto li = sink.left_intersections[i];
		auto ri = sink.right_intersections[i];
		sink.left_slices.push_back(merge_path_left.Slice(l_prev.first, l_prev.second, li.first, li.second));
		sink.right_slices.push_back(merge_path_right.Slice(r_prev.first, r_prev.second, ri.first, ri.second));
		// schedule a merge task on these slices
		sink.intermediate.back().push_back(make_unique<ContinuousBlock>(sink));
		auto new_task =
		    make_unique<PhysicalOrderMergeTask>(pipeline, context, sink, *sink.left_slices.back(),
		                                        *sink.right_slices.back(), *sink.intermediate.back().back());
		pipeline.total_tasks++;
		ts.ScheduleTask(pipeline.token, move(new_task));
		// set previous
		l_prev = li;
		r_prev = ri;
	}
	// all intersections were used to slice blocks, clear for the next iteration
	sink.left_intersections.clear();
	sink.right_intersections.clear();
	// skip to the next two blocks to merge
	sink.mp_block_idx += 2;
}

void PhysicalOrder::ScheduleMergeTasks(Pipeline &pipeline, ClientContext &context, GlobalOperatorState &state) {
	auto &sink = (OrderGlobalState &)state;
	auto &bm = BufferManager::GetBufferManager(context);

	// cleanup - move intermediate to result
	for (auto &cb : sink.continuous) {
		for (auto &block : cb->blocks) {
			bm.UnregisterBlock(block.block->BlockId(), true);
		}
	}
	sink.continuous.clear();
	for (auto &continuous_vec : sink.intermediate) {
		if (continuous_vec.size() == 1) {
			sink.continuous.push_back(move(continuous_vec[0]));
			continue;
		}
		sink.continuous.emplace_back(make_unique<ContinuousBlock>(sink));
		auto &combined_cb = *sink.continuous.back();
		for (auto &cb : continuous_vec) {
			combined_cb.blocks.insert(combined_cb.blocks.end(), cb->blocks.begin(), cb->blocks.end());
		}
		combined_cb.end = combined_cb.blocks.back().count;
	}
	sink.intermediate.clear();

	// finish pipeline if there is only one continuous block left
	if (sink.continuous.size() == 1) {
		pipeline.Finish();
		return;
	}

	// shuffle to (somewhat) prevent skew
	std::random_shuffle(sink.continuous.begin(), sink.continuous.end());

	// cannot merge last element if odd amount
	if (sink.continuous.size() % 2 == 1) {
		sink.intermediate.emplace_back();
		sink.intermediate.back().push_back(move(sink.continuous.back()));
		sink.continuous.pop_back();
	}

	auto &ts = TaskScheduler::GetScheduler(context);
	idx_t n_threads = ts.NumberOfThreads();
	if (sink.continuous.size() / 2 >= n_threads) {
		// easy: we can assign two blocks per thread
		for (idx_t i = 0; i < sink.continuous.size(); i += 2) {
			sink.intermediate.emplace_back();
			sink.intermediate.back().push_back(make_unique<ContinuousBlock>(sink));
			auto new_task =
			    make_unique<PhysicalOrderMergeTask>(pipeline, context, sink, *sink.continuous[i],
			                                        *sink.continuous[i + 1], *sink.intermediate.back().back());
			pipeline.total_tasks++;
			ts.ScheduleTask(pipeline.token, move(new_task));
		}
	} else {
		// balance the load using Merge Path
		sink.merge_path = true;
		sink.mp_block_idx = 0;
		ScheduleMergePathTasks(pipeline, context, state);
	}
}

void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	this->sink_state = move(state);
	auto &sink = (OrderGlobalState &)*this->sink_state;

	if (sink.intermediate.capacity() == 1) {
		// special case: only one block arrived, it was sorted but not re-ordered
		auto single_block = sink.intermediate.back().back().get();
		sink.continuous.push_back(make_unique<ContinuousBlock>(sink));
		single_block->PinBlock();
		single_block->FlushData(*sink.continuous.back());
		return;
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
	mutex lock;

	idx_t current_block;
	idx_t position;
};

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(*this, children[0].get());
}

void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_p);
	auto &sink = (OrderGlobalState &)*this->sink_state;
	lock_guard<mutex> glock(state->lock);

	D_ASSERT(sink.continuous.size() == 1);

	if (state->current_block >= sink.continuous[0]->blocks.size()) {
		state->finished = true;
		return;
	}

	idx_t position;
	RowDataBlock *block;
	{
		//        lock_guard<mutex> glock(state->lock);
		position = state->position;
		block = &sink.continuous[0]->blocks[state->current_block];
		state->position += STANDARD_VECTOR_SIZE;
		if (state->position >= block->count) {
			state->current_block++;
			state->position = 0;
		}
	}

	auto handle = sink.buffer_manager.Pin(block->block);
	auto dataptr = handle->node->buffer + position * sink.entry_size;

	data_ptr_t nullmask_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];

	// fetch the next vector of entries from the blocks
	idx_t next = MinValue<idx_t>(STANDARD_VECTOR_SIZE, block->count - position);
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
