#include "duckdb/execution/operator/order/physical_order.hpp"

#include "blockquicksort_wrapper.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t estimated_cardinality)
    : PhysicalSink(PhysicalOperatorType::ORDER_BY, move(types), estimated_cardinality), orders(move(orders)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct SortingState {
	SortingState(const vector<PhysicalType> &sorting_p_types, const vector<OrderByNullType> &null_orders,
	             const idx_t &validitymask_size, const idx_t &constant_entry_size, const idx_t &block_capacity,
	             const idx_t &positions_blocksize, const vector<int8_t> &is_asc, const vector<int8_t> &nulls_first)
	    : SORTING_P_TYPES(sorting_p_types), NULL_ORDERS(null_orders), VALIDITYMASK_SIZE(validitymask_size),
	      CONSTANT_ENTRY_SIZE(constant_entry_size), BLOCK_CAPACITY(block_capacity),
	      POSITIONS_BLOCKSIZE(positions_blocksize), IS_ASC(is_asc), NULLS_FIRST(nulls_first) {
	}
	const vector<PhysicalType> SORTING_P_TYPES;
	const vector<OrderByNullType> NULL_ORDERS;

	const idx_t VALIDITYMASK_SIZE;
	const idx_t CONSTANT_ENTRY_SIZE;
	const idx_t BLOCK_CAPACITY;
	const idx_t POSITIONS_BLOCKSIZE;

	const uint8_t VALID_BITS[8] = {1, 2, 4, 8, 16, 32, 64, 128};
	const vector<int8_t> IS_ASC;
	const vector<int8_t> NULLS_FIRST;
};

class OrderGlobalState : public GlobalOperatorState {
public:
	OrderGlobalState(PhysicalOrder &op, BufferManager &buffer_manager)
	    : op(op), buffer_manager(buffer_manager), merge_path(false) {
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
	idx_t validitymask_size;
	idx_t block_capacity;

	//! Ordered segments
	vector<unique_ptr<ContinuousBlock>> continuous;
	//! Intermediate results
	vector<vector<unique_ptr<ContinuousBlock>>> intermediate;

	void InitializeSortingState(const vector<idx_t> &sorting_p_sizes, const vector<OrderByNullType> &null_orders,
	                            const idx_t &constant_entry_size, const idx_t &positions_blocksize,
	                            const vector<int8_t> &is_asc, const vector<int8_t> &nulls_first) {
		sorting_state = make_unique<SortingState>(sorting_p_types, null_orders, validitymask_size, constant_entry_size,
		                                          block_capacity, positions_blocksize, is_asc, nulls_first);
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

	//! Holds a vector of incoming sorting columns
	DataChunk sort;

	//! Incoming data in row format
	RowChunk row_chunk;

	//! Allocate arrays for vector serialization
	const SelectionVector *sel_ptr = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t validitymask_locations[STANDARD_VECTOR_SIZE];
	idx_t entry_sizes[STANDARD_VECTOR_SIZE];

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
	state->validitymask_size = (state->sorting_p_types.size() + state->payload_p_types.size() + 7) / 8;
	state->block_capacity = SORTING_BLOCK_SIZE;

	// determine whether the entries are of variable size
	bool constant = true;
	idx_t entry_size = 0;
	for (auto &ptype : state->sorting_p_types) {
		constant = constant && TypeIsConstantSize(ptype);
		entry_size += GetTypeIdSize(ptype);
	}
	for (auto &ptype : state->payload_p_types) {
		constant = constant && TypeIsConstantSize(ptype);
		entry_size += GetTypeIdSize(ptype);
	}
	if (constant) {
		state->InitializeSortingState(sorting_p_sizes, null_orders, entry_size + state->validitymask_size, 0, is_asc,
		                              nulls_first);
	} else {
		idx_t positions_blocksize =
		    MaxValue((idx_t)Storage::BLOCK_ALLOC_SIZE, ((SORTING_BLOCK_SIZE / entry_size) + 1) * sizeof(idx_t));
		state->InitializeSortingState(sorting_p_sizes, null_orders, 0, positions_blocksize, is_asc, nulls_first);
	}

	return state;
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
	auto result = make_unique<OrderLocalState>(BufferManager::GetBufferManager(context.client));
	vector<LogicalType> types;
	for (auto &order : orders) {
		types.push_back(order.expression->return_type);
	}
	result->sort.Initialize(types);
	return result;
}

struct ContinuousBlock {
public:
	explicit ContinuousBlock(OrderGlobalState &state)
	    : start(0), state(state), sorting_state(*state.sorting_state), block_idx(0) {
	}
	vector<RowDataBlock> blocks;
	idx_t start;
	idx_t end;

	//! Used only for the initial merge after sorting in Sink/Combine
	shared_ptr<BlockHandle> sorted_indices_block = nullptr;
	idx_t *sorted_indices = nullptr;

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

	idx_t &EntrySize() {
		return current_entry_size;
	}

	bool Done() {
		return block_idx >= blocks.size();
	}

	void Advance() {
		if (entry_idx < block_end - 1) {
			entry_idx++;
			if (!sorted_indices && sorting_state.CONSTANT_ENTRY_SIZE) {
				dataptr += sorting_state.CONSTANT_ENTRY_SIZE;
			} else {
				InitializeEntry();
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
		// initialize block boundaries
		entry_idx = block_idx == 0 ? start : 0;
		block_end = block_idx == blocks.size() - 1 ? end : blocks[block_idx].count;
		// pin data block
		handle = state.buffer_manager.Pin(blocks[block_idx].block);
		base_dataptr = handle->node->buffer;
		// pin positions block (if non-const length)
		if (sorting_state.CONSTANT_ENTRY_SIZE) {
			current_entry_size = sorting_state.CONSTANT_ENTRY_SIZE;
		} else {
			positions_handle = state.buffer_manager.Pin(blocks[block_idx].entry_positions);
			entry_positions = ((idx_t *)positions_handle->node->buffer);
		}
		// pin sorted indices (if any)
		if (sorted_indices_block) {
			sorted_indices_handle = state.buffer_manager.Pin(sorted_indices_block);
			sorted_indices = (idx_t *)sorted_indices_handle->node->buffer;
		}
		// initialize current entry
		InitializeEntry();
	}

	void FlushData(ContinuousBlock &target) {
		RowDataBlock *write_block = nullptr;
		unique_ptr<BufferHandle> write_handle;
		data_ptr_t write_ptr;
		unique_ptr<BufferHandle> write_positions_handle = nullptr;
		idx_t *write_positions;

		if (!target.blocks.empty()) {
			write_block = &target.blocks.back();
			write_handle = state.buffer_manager.Pin(write_block->block);
			write_ptr = write_handle->node->buffer + write_block->byte_offset;
			if (!sorting_state.CONSTANT_ENTRY_SIZE) {
				write_positions_handle = state.buffer_manager.Pin(write_block->entry_positions);
				write_positions = ((idx_t *)write_positions_handle->node->buffer) + write_block->count;
			}
		}

		// flush data of last block(s)
		while (!Done()) {
			if (!write_block || write_block->byte_offset + EntrySize() > write_block->byte_capacity) {
				// initialize new blocks to write to
				target.blocks.emplace_back(state.buffer_manager, sorting_state.BLOCK_CAPACITY,
				                           sorting_state.CONSTANT_ENTRY_SIZE, sorting_state.POSITIONS_BLOCKSIZE);
				write_block = &target.blocks.back();
				write_handle = state.buffer_manager.Pin(write_block->block);
				write_ptr = write_handle->node->buffer;
				if (!sorting_state.CONSTANT_ENTRY_SIZE) {
					write_positions_handle = state.buffer_manager.Pin(write_block->entry_positions);
					write_positions = (idx_t *)write_positions_handle->node->buffer;
					*write_positions = 0;
					write_positions++;
				}
			}
			memcpy(write_ptr, DataPtr(), EntrySize());
			write_block->byte_offset += EntrySize();
			if (!sorting_state.CONSTANT_ENTRY_SIZE) {
				*write_positions = write_block->byte_offset;
				write_positions++;
			}
			write_ptr += EntrySize();
			write_block->count++;
			Advance();
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
		result->sorted_indices = sorted_indices;
		return result;
	}

private:
	OrderGlobalState &state;
	const SortingState &sorting_state;

	idx_t block_idx;
	idx_t entry_idx;
	idx_t block_end;

	unique_ptr<BufferHandle> handle;
	data_ptr_t base_dataptr;
	data_ptr_t dataptr;

	unique_ptr<BufferHandle> positions_handle;
	idx_t *entry_positions;
	idx_t current_entry_size;

	unique_ptr<BufferHandle> sorted_indices_handle;

	void InitializeEntry() {
		idx_t actual_entry_idx = sorted_indices ? sorted_indices[entry_idx] : entry_idx;
		if (sorting_state.CONSTANT_ENTRY_SIZE) {
			dataptr = base_dataptr + actual_entry_idx * sorting_state.CONSTANT_ENTRY_SIZE;
		} else {
			dataptr = base_dataptr + entry_positions[actual_entry_idx];
			current_entry_size = entry_positions[actual_entry_idx + 1] - entry_positions[actual_entry_idx];
		}
	}
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

static int32_t CompareValue(const data_ptr_t &l_validitymask, const data_ptr_t &r_validitymask, data_ptr_t &l_val,
                            data_ptr_t &r_val, const idx_t &sort_idx, const SortingState &state, idx_t &l_size,
                            idx_t &r_size) {
	auto byte_offset = sort_idx / 8;
	auto offset_in_byte = sort_idx % 8;
	auto left_valid = *(l_validitymask + byte_offset) & state.VALID_BITS[offset_in_byte];
	auto right_valid = *(r_validitymask + byte_offset) & state.VALID_BITS[offset_in_byte];

	if (TypeIsConstantSize(state.SORTING_P_TYPES[sort_idx])) {
		l_size = GetTypeIdSize(state.SORTING_P_TYPES[sort_idx]);
		r_size = GetTypeIdSize(state.SORTING_P_TYPES[sort_idx]);
	} else {
		switch (state.SORTING_P_TYPES[sort_idx]) {
		case PhysicalType::VARCHAR: {
			l_size = string_t::PREFIX_LENGTH + Load<idx_t>(l_val);
			r_size = string_t::PREFIX_LENGTH + Load<idx_t>(r_val);
			break;
		}
		default:
			throw NotImplementedException("Type for comparison");
		}
	}

	if (!left_valid && !right_valid) {
		return 0;
	} else if (!right_valid) {
		return state.NULLS_FIRST[sort_idx];
	} else if (!left_valid) {
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
	case PhysicalType::INTERVAL:
		return TemplatedCompareValue<interval_t>(l_val, r_val);
	case PhysicalType::VARCHAR: {
		auto l_str_size = l_size - string_t::PREFIX_LENGTH;
		auto r_str_size = r_size - string_t::PREFIX_LENGTH;
		l_val += string_t::PREFIX_LENGTH;
		r_val += string_t::PREFIX_LENGTH;
		auto result = strncmp((const char *)l_val, (const char *)r_val, MinValue(l_str_size, r_str_size));
		l_val += l_str_size;
		r_val += r_str_size;
		return result;
	}
	default:
		throw NotImplementedException("Type for comparison");
	}
}

static int CompareTuple(const data_ptr_t &l_start, const data_ptr_t &r_start, const SortingState &state) {
	data_ptr_t l_val = l_start + state.VALIDITYMASK_SIZE;
	data_ptr_t r_val = r_start + state.VALIDITYMASK_SIZE;
	idx_t l_size = 0;
	idx_t r_size = 0;
	for (idx_t i = 0; i < state.SORTING_P_TYPES.size(); i++) {
		auto comp_res = CompareValue(l_start, r_start, l_val, r_val, i, state, l_size, r_size);
		if (comp_res == 0) {
			l_val += l_size;
			r_val += r_size;
			continue;
		}
		return comp_res * state.IS_ASC[i];
	}
	return 0;
}

static void Sort(ContinuousBlock &cb, OrderGlobalState &state) {
	D_ASSERT(cb.blocks.size() == 1);
	auto &block = cb.blocks[0];
	cb.end = block.count;

	// data buffer
	auto handle = state.buffer_manager.Pin(block.block);
	data_ptr_t dataptr = handle->node->buffer;

	// fetch pointers to entries in the blocks
	const auto &sorting_state = *state.sorting_state;
	auto key_locations = unique_ptr<data_ptr_t[]>(new data_ptr_t[block.count]);
	if (!sorting_state.CONSTANT_ENTRY_SIZE) {
		auto positions_handle = state.buffer_manager.Pin(block.entry_positions);
		auto entry_positions = (idx_t *)positions_handle->node->buffer;
		for (idx_t i = 0; i < block.count; i++) {
			key_locations[i] = dataptr + entry_positions[i];
		}
	} else {
		for (idx_t i = 0; i < block.count; i++) {
			key_locations[i] = dataptr;
			dataptr += sorting_state.CONSTANT_ENTRY_SIZE;
		}
	}

	cb.sorted_indices_block = state.buffer_manager.RegisterMemory(
	    MaxValue((idx_t)Storage::BLOCK_ALLOC_SIZE, block.count * sizeof(idx_t)), false);
	auto sorted_indices_handle = state.buffer_manager.Pin(cb.sorted_indices_block);
	cb.sorted_indices = (idx_t *)sorted_indices_handle->node->buffer;
	for (idx_t i = 0; i < block.count; i++) {
		cb.sorted_indices[i] = i;
	}

	BlockQuickSort::Sort(cb.sorted_indices, cb.sorted_indices + block.count,
	                     [&key_locations, &sorting_state](const idx_t &l, const idx_t &r) {
		                     return CompareTuple(key_locations[l], key_locations[r], sorting_state) <= 0;
	                     });
}

static void IncrementEntrySizes(DataChunk &chunk, OrderGlobalState &gstate, OrderLocalState &lstate, bool payload) {
	VectorData vdata;
	for (idx_t col_idx = 0; col_idx < chunk.data.size(); col_idx++) {
		if (payload && gstate.p_to_s.find(col_idx) != gstate.p_to_s.end()) {
			// this column will be serialized as a sorting column
			continue;
		}

		// constant size columns
		if (TypeIsConstantSize(chunk.data[col_idx].GetType().InternalType())) {
			idx_t col_size = GetTypeIdSize(chunk.data[col_idx].GetType().InternalType());
			for (idx_t i = 0; i < chunk.size(); i++) {
				lstate.entry_sizes[i] += col_size;
			}
		}

		// var size columns
		chunk.data[col_idx].Orrify(chunk.size(), vdata);
		switch (chunk.data[col_idx].GetType().InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = (string_t *)vdata.data;
			for (idx_t i = 0; i < chunk.size(); i++) {
				lstate.entry_sizes[i] += string_t::PREFIX_LENGTH + strings[vdata.sel->get_index(i)].GetSize();
			}
			break;
		}
		default:
			throw NotImplementedException("Unimplemented type for ORDER BY!");
		}
	}
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p,
                         DataChunk &input) {
	auto &gstate = (OrderGlobalState &)state;
	auto &lstate = (OrderLocalState &)lstate_p;
	const auto &sorting_state = *gstate.sorting_state;

	if (lstate.row_chunk.blocks.empty()) {
		// init using global state
		lstate.row_chunk.block_capacity = sorting_state.BLOCK_CAPACITY;
	}

	// obtain sorting columns
	auto &sort = lstate.sort;
	gstate.executor.Execute(input, sort);

	if (!sorting_state.CONSTANT_ENTRY_SIZE) {
		// compute the size of each entry, starting with the validitymask size
		std::fill_n(lstate.entry_sizes, STANDARD_VECTOR_SIZE, sorting_state.VALIDITYMASK_SIZE);
		// now the sorting and payload columns
		IncrementEntrySizes(sort, gstate, lstate, false);
		IncrementEntrySizes(input, gstate, lstate, true);
	}

	// build the block
	const idx_t block_count_before = lstate.row_chunk.Size();
	lstate.row_chunk.Build(input.size(), lstate.validitymask_locations, lstate.entry_sizes,
	                       sorting_state.CONSTANT_ENTRY_SIZE, sorting_state.POSITIONS_BLOCKSIZE);
	for (idx_t i = 0; i < sort.size(); i++) {
		// initialize validitymasks to 1 and initialize key locations
		memset(lstate.validitymask_locations[i], -1, sorting_state.VALIDITYMASK_SIZE);
		lstate.key_locations[i] = lstate.validitymask_locations[i] + sorting_state.VALIDITYMASK_SIZE;
	}
	// serialize sorting columns to row-wise format
	for (idx_t i = 0; i < sort.data.size(); i++) {
		lstate.row_chunk.SerializeVector(sort.data[i], sort.size(), *lstate.sel_ptr, sort.size(), i,
		                                 lstate.key_locations, lstate.validitymask_locations);
	}
	// serialize payload columns to row-wise format
	for (idx_t i = 0; i < input.data.size(); i++) {
		if (gstate.p_to_s.find(i) != gstate.p_to_s.end()) {
			// this column is already serialized as a sorting column
			continue;
		}
		lstate.row_chunk.SerializeVector(input.data[i], input.size(), *lstate.sel_ptr, input.size(),
		                                 gstate.p_to_p[i] + sort.size(), lstate.key_locations,
		                                 lstate.validitymask_locations);
	}
	// sort the block if it is full
	if (block_count_before != 0 && lstate.row_chunk.Size() > block_count_before) {
		lstate.continuous.push_back(make_unique<ContinuousBlock>(gstate));
		auto &new_cb = *lstate.continuous.back();
		new_cb.blocks.push_back(lstate.row_chunk.blocks[lstate.row_chunk.Size() - 2]);
		Sort(new_cb, gstate);
	}
}

void PhysicalOrder::Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p) {
	auto &gstate = (OrderGlobalState &)state;
	auto &lstate = (OrderLocalState &)lstate_p;

	if (lstate.row_chunk.blocks.empty()) {
		return;
	}

	ContinuousBlock *unsorted_block;
	{
		// append to global state. this initializes gstate RowChunks if empty
		lock_guard<mutex> glock(gstate.lock);
		for (auto &c : lstate.continuous) {
			gstate.intermediate.emplace_back();
			gstate.intermediate.back().push_back(move(c));
		}
		// last block of each local state is always unsorted
		gstate.intermediate.emplace_back();
		gstate.intermediate.back().push_back(make_unique<ContinuousBlock>(gstate));
		unsorted_block = gstate.intermediate.back().back().get();
		unsorted_block->blocks.push_back(lstate.row_chunk.blocks.back());
	}

	Sort(*unsorted_block, gstate);
}

class CBScanState {
public:
	//! Used by the MergePathTask
	CBScanState(ContinuousBlock &cb, BufferManager &buffer_manager, SortingState &sorting_state)
	    : cb(cb), buffer_manager(buffer_manager), sorting_state(sorting_state) {
		block_entry = std::make_pair(0, 0);
	}

	data_ptr_t DataPtrAt(idx_t global_index) {
		block_entry = cb.GlobalToLocalIndex(global_index);
		if (!block || block->block->BlockId() != cb.blocks[block_entry.first].block->BlockId()) {
			// pin data block
			block = &cb.blocks[block_entry.first];
			block_handle = buffer_manager.Pin(block->block);
			baseptr = block_handle->node->buffer;

			// pin positions block
			if (!sorting_state.CONSTANT_ENTRY_SIZE) {
				positions_handle = buffer_manager.Pin(block->entry_positions);
				entry_positions = (idx_t *)positions_handle->node->buffer;
			}

			if (cb.sorted_indices_block) {
				sorted_indices_handle = buffer_manager.Pin(cb.sorted_indices_block);
				sorted_indices = (idx_t *)sorted_indices_handle->node->buffer;
			}
		}

		idx_t actual_idx = sorted_indices ? sorted_indices[block_entry.second] : block_entry.second;
		if (sorting_state.CONSTANT_ENTRY_SIZE) {
			return baseptr + actual_idx * sorting_state.CONSTANT_ENTRY_SIZE;
		} else {
			return baseptr + entry_positions[actual_idx];
		}
	}

	//! The block that is being scanned
	ContinuousBlock &cb;
	//! The local index of the last entry that was accessed using the global index
	std::pair<idx_t, idx_t> block_entry;

private:
	BufferManager &buffer_manager;
	const SortingState &sorting_state;

	//! The last block that was accessed
	RowDataBlock *block = nullptr;

	//! Data buffer
	unique_ptr<BufferHandle> block_handle;
	data_ptr_t baseptr;

	//! Positions buffer
	unique_ptr<BufferHandle> positions_handle;
	idx_t *entry_positions;

	//! Sorted indices buffer
	unique_ptr<BufferHandle> sorted_indices_handle;
	idx_t *sorted_indices = nullptr;
};

class PhysicalOrderMergePathTask : public Task {
public:
	PhysicalOrderMergePathTask(Pipeline &parent, ClientContext &context, OrderGlobalState &state, idx_t sum,
	                           ContinuousBlock &left, ContinuousBlock &right, idx_t result_idx)
	    : parent(parent), context(context), state(state),
	      l_state(left, BufferManager::GetBufferManager(context), *state.sorting_state),
	      r_state(right, BufferManager::GetBufferManager(context), *state.sorting_state), sum(sum),
	      result_idx(result_idx) {
	}

	void Execute() override {
		const auto &sorting_state = *state.sorting_state;
		const idx_t l_count = l_state.cb.Count();
		const idx_t r_count = r_state.cb.Count();

		// determine bounds of the intersection
		idx_t l_lower = r_count > sum ? 0 : sum - r_count;
		idx_t r_upper = MinValue(sum, r_count);

		// binary search
		idx_t search_start = 0;
		idx_t search_end = MinValue(sum, l_count) - l_lower;
		while (search_start <= search_end) {
			idx_t middle = (search_start + search_end) / 2;

			// compare tuples and update search boundaries
			auto comp_res =
			    CompareTuple(l_state.DataPtrAt(l_lower + middle), r_state.DataPtrAt(r_upper - middle), sorting_state);
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
		state.left_intersections[result_idx] = l_state.block_entry;
		state.right_intersections[result_idx] = r_state.block_entry;

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
	OrderGlobalState &state;

	CBScanState l_state;
	CBScanState r_state;
	idx_t sum;
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

		// pointer to the block that will be written to
		RowDataBlock *write_block = nullptr;
		unique_ptr<BufferHandle> write_handle;
		data_ptr_t write_ptr;
		unique_ptr<BufferHandle> write_pos_handle;
		idx_t *entry_positions;

		ContinuousBlock *source;
		while (!left.Done() && !right.Done()) {
			// determine where to copy from
			if (CompareTuple(left.DataPtr(), right.DataPtr(), sorting_state) <= 0) {
				source = &left;
			} else {
				source = &right;
			}
			if (!write_block || write_block->byte_offset + source->EntrySize() > write_block->byte_capacity) {
				// initialize new blocks to write to if the current entry does not fit
				result.blocks.emplace_back(buffer_manager, sorting_state.BLOCK_CAPACITY,
				                           sorting_state.CONSTANT_ENTRY_SIZE, sorting_state.POSITIONS_BLOCKSIZE);
				write_block = &result.blocks.back();
				write_handle = buffer_manager.Pin(write_block->block);
				write_ptr = write_handle->node->buffer;
				if (!sorting_state.CONSTANT_ENTRY_SIZE) {
					write_pos_handle = buffer_manager.Pin(write_block->entry_positions);
					entry_positions = (idx_t *)write_pos_handle->node->buffer;
				}
			}
			// copy the entry
			memcpy(write_ptr, source->DataPtr(), source->EntrySize());
			if (!sorting_state.CONSTANT_ENTRY_SIZE) {
				*entry_positions = write_block->byte_offset;
				entry_positions++;
			}
			// write block bookkeeping
			write_block->byte_offset += source->EntrySize();
			write_ptr += source->EntrySize();
			write_block->count++;
			// advance the block we read from
			source->Advance();
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
	    : PhysicalOperatorState(op, child), block_idx(0), entry_idx(0) {
	}

	unique_ptr<BufferHandle> handle = nullptr;
	data_ptr_t base_dataptr;

	unique_ptr<BufferHandle> positions_handle = nullptr;
	idx_t *entry_positions;

	RowDataBlock *InitBlock(ContinuousBlock &cb, BufferManager &buffer_manager, const SortingState &sorting_state) {
		auto block = &cb.blocks[block_idx];

		handle = buffer_manager.Pin(block->block);
		base_dataptr = handle->node->buffer;

		if (!sorting_state.CONSTANT_ENTRY_SIZE) {
			positions_handle = buffer_manager.Pin(block->entry_positions);
			entry_positions = (idx_t *)positions_handle->node->buffer;
		}

		return block;
	}

	data_ptr_t validitymask_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];

	idx_t block_idx;
	idx_t entry_idx;
};

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(*this, children[0].get());
}

void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_p);
	auto &sink = (OrderGlobalState &)*this->sink_state;
	const auto &sorting_state = *sink.sorting_state;

	D_ASSERT(sink.continuous.size() == 1);

	auto &cb = *sink.continuous[0];
	RowDataBlock *block = state->InitBlock(cb, BufferManager::GetBufferManager(context.client), sorting_state);

	idx_t tuples = 0;
	while (tuples < STANDARD_VECTOR_SIZE && state->block_idx < cb.blocks.size()) {
		// init the next block if needed
		if (state->entry_idx >= block->count) {
			state->block_idx++;
			if (state->block_idx >= cb.blocks.size()) {
				state->finished = true;
				break;
			}
			state->entry_idx = 0;
			block = state->InitBlock(cb, BufferManager::GetBufferManager(context.client), sorting_state);
		}

		// fetch the next vector of entries from the blocks
		idx_t next = MinValue<idx_t>(STANDARD_VECTOR_SIZE - tuples, block->count - state->entry_idx);
		data_ptr_t dataptr = state->base_dataptr;
		if (sorting_state.CONSTANT_ENTRY_SIZE) {
			dataptr += state->entry_idx * sorting_state.CONSTANT_ENTRY_SIZE;
			for (idx_t i = tuples; i < tuples + next; i++) {
				state->validitymask_locations[i] = dataptr;
				state->key_locations[i] = dataptr + sorting_state.VALIDITYMASK_SIZE;
				dataptr += sorting_state.CONSTANT_ENTRY_SIZE;
			}
		} else {
			const idx_t pos = state->entry_idx;
			for (idx_t i = tuples; i < tuples + next; i++) {
				state->validitymask_locations[i] = dataptr + state->entry_positions[pos + i];
				state->key_locations[i] = state->validitymask_locations[i] + sorting_state.VALIDITYMASK_SIZE;
			}
		}
		tuples += next;
		state->entry_idx += next;
	}
	chunk.SetCardinality(tuples);

	// deserialize sorting columns (if needed)
	for (idx_t sort_idx = 0; sort_idx < sink.sorting_p_types.size(); sort_idx++) {
		if (sink.s_to_p.find(sort_idx) == sink.s_to_p.end()) {
			// sorting column does not need to be output, move pointers
			RowChunk::SkipOverType(sink.sorting_p_types[sort_idx], tuples, state->key_locations);
		} else {
			// sorting column needs to be output
			RowChunk::DeserializeIntoVector(chunk.data[sink.s_to_p[sort_idx]], tuples, sort_idx, state->key_locations,
			                                state->validitymask_locations);
		}
	}
	// deserialize payload columns
	for (idx_t payl_idx = 0; payl_idx < sink.payload_p_types.size(); payl_idx++) {
		RowChunk::DeserializeIntoVector(chunk.data[sink.p_to_p[payl_idx]], tuples,
		                                sink.sorting_p_types.size() + payl_idx, state->key_locations,
		                                state->validitymask_locations);
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
