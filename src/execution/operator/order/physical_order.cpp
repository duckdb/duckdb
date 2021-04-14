#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t estimated_cardinality)
    : PhysicalSink(PhysicalOperatorType::ORDER_BY, move(types), estimated_cardinality), orders(move(orders)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct SortingState {
	const idx_t ENTRY_SIZE;
	const idx_t COMP_SIZE;
	const bool ALL_CONSTANT;

	const vector<OrderType> ORDER_TYPES;
	const vector<OrderByNullType> ORDER_BY_NULL_TYPES;
	const vector<LogicalType> TYPES;
	const vector<BaseStatistics *> STATS;

	const vector<bool> HAS_NULL;
	const vector<bool> CONSTANT_SIZE;
	const vector<idx_t> COL_SIZE;

	const vector<idx_t> ROWCHUNK_INIT_SIZES;
};

struct PayloadState {
	const bool HAS_VARIABLE_SIZE;
	const idx_t VALIDITYMASK_SIZE;
	const idx_t ENTRY_SIZE;

	const idx_t ROWCHUNK_INIT_SIZE;
};

class OrderGlobalState : public GlobalOperatorState {
public:
	explicit OrderGlobalState(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
	}
	//! The lock for updating the order global state
	std::mutex lock;
	//! The buffer manager
	BufferManager &buffer_manager;

	//! Constants concerning sorting and/or payload data
	unique_ptr<SortingState> sorting_state;
	unique_ptr<PayloadState> payload_state;

	//! Sorted data
	vector<unique_ptr<ContinuousBlock>> sorted_blocks;
	vector<unique_ptr<ContinuousBlock>> sorted_blocks_temp;

	//! Total count - set after PhysicalOrder::Finalize is called
	idx_t total_count;

	//! Capacity/entry sizes used to initialize blocks
	idx_t sorting_block_capacity;
	vector<std::pair<idx_t, idx_t>> var_sorting_data_block_dims;
	vector<idx_t> var_sorting_offset_block_capacity;
	std::pair<idx_t, idx_t> payload_data_block_dims;
	idx_t payload_offset_block_capacity;
};

class OrderLocalState : public LocalSinkState {
public:
	OrderLocalState() : initialized(false) {
	}

	//! Whether this local state has been initialized
	bool initialized;

	//! Local copy of the executor
	ExpressionExecutor executor;

	//! Holds a vector of incoming sorting columns
	DataChunk sort;

	void Initialize(BufferManager &buffer_manager, const SortingState &sorting_state,
	                const PayloadState &payload_state) {
		// sorting block
		idx_t vectors_per_block =
		    (Storage::BLOCK_ALLOC_SIZE / sorting_state.ENTRY_SIZE + STANDARD_VECTOR_SIZE) / STANDARD_VECTOR_SIZE;
		sorting_block =
		    make_unique<RowChunk>(buffer_manager, vectors_per_block * STANDARD_VECTOR_SIZE, sorting_state.ENTRY_SIZE);
		// variable sorting column blocks
		for (idx_t i = 0; i < sorting_state.CONSTANT_SIZE.size(); i++) {
			if (sorting_state.CONSTANT_SIZE[i]) {
				var_sorting_blocks.push_back(nullptr);
				var_sorting_sizes.push_back(nullptr);
			} else {
				var_sorting_blocks.push_back(
				    make_unique<RowChunk>(buffer_manager, sorting_state.ROWCHUNK_INIT_SIZES[i] / 8, 8));
				var_sorting_sizes.push_back(make_unique<RowChunk>(
				    buffer_manager, (idx_t)Storage::BLOCK_ALLOC_SIZE / sizeof(idx_t) + 1, sizeof(idx_t)));
			}
		}
		// payload block
		if (payload_state.HAS_VARIABLE_SIZE) {
			payload_block = make_unique<RowChunk>(buffer_manager, payload_state.ROWCHUNK_INIT_SIZE / 32, 32);
			sizes_block = make_unique<RowChunk>(buffer_manager, (idx_t)Storage::BLOCK_ALLOC_SIZE / sizeof(idx_t) + 1,
			                                    sizeof(idx_t));
		} else {
			payload_block = make_unique<RowChunk>(
			    buffer_manager, payload_state.ROWCHUNK_INIT_SIZE / payload_state.ENTRY_SIZE, payload_state.ENTRY_SIZE);
		}
		initialized = true;
	}

	//! Sorting columns, and variable size sorting data (if any)
	unique_ptr<RowChunk> sorting_block = nullptr;
	vector<unique_ptr<RowChunk>> var_sorting_blocks;
	vector<unique_ptr<RowChunk>> var_sorting_sizes;

	//! Payload data (and payload entry sizes if there is variable size data)
	unique_ptr<RowChunk> payload_block = nullptr;
	unique_ptr<RowChunk> sizes_block = nullptr;

	//! Sorted data
	vector<unique_ptr<ContinuousBlock>> sorted_blocks;

	//! Constant buffers allocated for vector serialization
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
	vector<bool> has_null;
	vector<bool> constant_size;
	vector<idx_t> col_sizes;
	vector<idx_t> rowchunk_init_sizes;
	for (auto &order : orders) {
		// global state ExpressionExecutor
		auto &expr = *order.expression;

		// sorting state
		order_types.push_back(order.type);
		order_by_null_types.push_back(order.null_order);
		types.push_back(expr.return_type);
		if (expr.stats) {
			stats.push_back(expr.stats.get());
		} else {
			stats.push_back(nullptr);
		}

		// compute column sizes
		auto physical_type = expr.return_type.InternalType();
		constant_size.push_back(TypeIsConstantSize(physical_type));
		idx_t col_size = GetTypeIdSize(expr.return_type.InternalType());

		// TODO: make use of statistics
		if (!TypeIsConstantSize(physical_type)) {
			switch (physical_type) {
			case PhysicalType::VARCHAR:
				col_size = StringStatistics::MAX_STRING_MINMAX_SIZE;
				break;
			default:
				// do nothing
				break;
			}
		}
		has_null.push_back(true);

		// increment entry size with the column size
		if (has_null.back()) {
			col_size++;
		}
		entry_size += col_size;
		col_sizes.push_back(col_size);

		// create RowChunks for variable size sorting columns in order to resolve
		if (TypeIsConstantSize(physical_type)) {
			rowchunk_init_sizes.push_back(0);
		} else {
			// besides the prefix, variable size sorting columns are also fully serialized, along with offsets
			// we have to assume a large variable size, otherwise a single large variable entry may not fit in a block
			// 1 << 23 = 8MB
			rowchunk_init_sizes.push_back((1 << 23) / 8);
		}
	}
	// make room for an 'index' column at the end
	idx_t comp_size = entry_size;
	entry_size += sizeof(idx_t);
	bool all_constant = true;
	for (auto constant : constant_size) {
		all_constant = all_constant && constant;
	}
	state->sorting_state = unique_ptr<SortingState>(new SortingState {entry_size, comp_size, all_constant, order_types,
	                                                                  order_by_null_types, types, stats, has_null,
	                                                                  constant_size, col_sizes, rowchunk_init_sizes});

	// init payload state
	entry_size = 0;
	idx_t validitymask_size = (children[0]->types.size() + 7) / 8;
	entry_size += validitymask_size;
	bool variable_payload_size = false;
	idx_t var_columns = 0;
	for (auto &type : children[0]->types) {
		auto physical_type = type.InternalType();
		if (TypeIsConstantSize(physical_type)) {
			entry_size += GetTypeIdSize(physical_type);
		} else {
			variable_payload_size = true;
			var_columns++;
		}
	}
	idx_t rowchunk_init_size;
	if (variable_payload_size) {
		rowchunk_init_size = entry_size + var_columns * (1 << 23);
	} else {
		idx_t vectors_per_block =
		    (Storage::BLOCK_ALLOC_SIZE / entry_size + STANDARD_VECTOR_SIZE) / STANDARD_VECTOR_SIZE;
		rowchunk_init_size = vectors_per_block * STANDARD_VECTOR_SIZE * entry_size;
	}
	state->payload_state = unique_ptr<PayloadState>(
	    new PayloadState {variable_payload_size, validitymask_size, entry_size, rowchunk_init_size});
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) {
	auto result = make_unique<OrderLocalState>();
	vector<LogicalType> types;
	for (auto &order : orders) {
		types.push_back(order.expression->return_type);
		result->executor.AddExpression(*order.expression);
	}
	result->sort.Initialize(types);
	return move(result);
}

void PhysicalOrder::Sink(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p,
                         DataChunk &input) {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;
	const auto &sorting_state = *gstate.sorting_state;
	const auto &payload_state = *gstate.payload_state;

	if (!lstate.initialized) {
		lstate.Initialize(BufferManager::GetBufferManager(context.client), *gstate.sorting_state,
		                  *gstate.payload_state);
	}

	// obtain sorting columns
	auto &sort = lstate.sort;
	lstate.executor.Execute(input, sort);

	// build and serialize sorting data
	lstate.sorting_block->Build(sort.size(), lstate.key_locations, nullptr);
	for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
		bool has_null = sorting_state.HAS_NULL[sort_col];
		bool nulls_first = sorting_state.ORDER_BY_NULL_TYPES[sort_col] == OrderByNullType::NULLS_FIRST;
		bool desc = sorting_state.ORDER_TYPES[sort_col] == OrderType::DESCENDING;
		idx_t size_in_bytes = StringStatistics::MAX_STRING_MINMAX_SIZE; // TODO: use actual string statistics
		lstate.sorting_block->SerializeVectorSortable(sort.data[sort_col], sort.size(), *lstate.sel_ptr, sort.size(),
		                                              lstate.key_locations, desc, has_null, nulls_first, size_in_bytes);
	}

	// also fully serialize variable size sorting columns
	for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
		if (TypeIsConstantSize(sort.data[sort_col].GetType().InternalType())) {
			continue;
		}
		auto &var_sizes = *lstate.var_sorting_sizes[sort_col];
		auto &var_block = *lstate.var_sorting_blocks[sort_col];
		// compute entry sizes
		std::fill_n(lstate.entry_sizes, input.size(), 0);
		RowChunk::ComputeEntrySizes(sort.data[sort_col], lstate.entry_sizes, sort.size());
		// build and serialize entry sizes
		var_sizes.Build(sort.size(), lstate.key_locations, nullptr);
		for (idx_t i = 0; i < input.size(); i++) {
			Store<idx_t>(lstate.entry_sizes[i], lstate.key_locations[i]);
		}
		// build and serialize variable size entries
		var_block.Build(sort.size(), lstate.key_locations, lstate.entry_sizes);
		var_block.SerializeVector(sort.data[sort_col], sort.size(), *lstate.sel_ptr, input.size(), 0,
		                          lstate.key_locations, nullptr);
	}

	// compute entry sizes of payload columns if there are variable size columns
	if (payload_state.HAS_VARIABLE_SIZE) {
		RowChunk::ComputeEntrySizes(input, lstate.entry_sizes, payload_state.ENTRY_SIZE);
		lstate.sizes_block->Build(input.size(), lstate.key_locations, nullptr);
		for (idx_t i = 0; i < input.size(); i++) {
			Store<idx_t>(lstate.entry_sizes[i], lstate.key_locations[i]);
		}
		lstate.payload_block->Build(input.size(), lstate.key_locations, lstate.entry_sizes);
	} else {
		lstate.payload_block->Build(input.size(), lstate.key_locations, nullptr);
	}

	// serialize payload data
	for (idx_t i = 0; i < input.size(); i++) {
		memset(lstate.key_locations[i], -1, payload_state.VALIDITYMASK_SIZE);
		lstate.validitymask_locations[i] = lstate.key_locations[i];
		lstate.key_locations[i] += payload_state.VALIDITYMASK_SIZE;
	}
	for (idx_t payl_col = 0; payl_col < input.ColumnCount(); payl_col++) {
		lstate.payload_block->SerializeVector(input.data[payl_col], input.size(), *lstate.sel_ptr, input.size(),
		                                      payl_col, lstate.key_locations, lstate.validitymask_locations);
	}

	// when sorting data reaches a certain size, we sort it
	if (lstate.sorting_block->count * sorting_state.ENTRY_SIZE > SORTING_BLOCK_SIZE) {
		SortLocalState(context.client, lstate, *gstate.sorting_state, *gstate.payload_state);
	}
}

void PhysicalOrder::Combine(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p) {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;

	if (!lstate.sorting_block) {
		return;
	}

	SortLocalState(context.client, lstate, *gstate.sorting_state, *gstate.payload_state);

	lock_guard<mutex> append_lock(gstate.lock);
	for (auto &cb : lstate.sorted_blocks) {
		gstate.sorted_blocks.push_back(move(cb));
	}
}

struct ContinuousChunk {
public:
	ContinuousChunk(BufferManager &buffer_manager, bool constant_size, idx_t entry_size = 0)
	    : buffer_manager(buffer_manager), CONSTANT_SIZE(constant_size), ENTRY_SIZE(entry_size), data_block_idx(0),
	      data_entry_idx(0), offset_block_idx(0), offset_entry_idx(0) {
	}

	idx_t Count() {
		idx_t count = std::accumulate(data_blocks.begin(), data_blocks.end(), 0,
		                              [](idx_t a, const RowDataBlock &b) { return a + b.count; });
#ifdef DEBUG
		if (!CONSTANT_SIZE) {
			D_ASSERT(count == std::accumulate(offset_blocks.begin(), offset_blocks.end(), (idx_t)0,
			                                  [](idx_t a, const RowDataBlock &b) { return a + b.count; }));
		}
#endif
		return count;
	}
	void InitializeWrite(const std::pair<idx_t, idx_t> dims_p, const idx_t offset_capacity_p) {
		data_dims = dims_p;
		offset_capacity = offset_capacity_p;
		CreateDataBlock();
	}

	// TODO: use a proper capacity/entry size instead of copying from this other dude
	void CreateDataBlock() {
		data_blocks.emplace_back(buffer_manager, data_dims.first, data_dims.second);
	}

	void CreateOffsetBlock() {
		offset_blocks.emplace_back(buffer_manager, offset_capacity, sizeof(idx_t));
	}

private:
    //! Buffer manager and constants
    BufferManager &buffer_manager;

    std::pair<idx_t, idx_t> data_dims;
    idx_t offset_capacity;

public:
    const bool CONSTANT_SIZE;
    const idx_t ENTRY_SIZE;

	//! Data and offset blocks
	vector<RowDataBlock> data_blocks;
	vector<RowDataBlock> offset_blocks;

	//! Data
	idx_t data_block_idx;
	idx_t data_entry_idx;

	//! Offsets (if any)
	idx_t offset_block_idx;
	idx_t offset_entry_idx;
};

struct ContinuousBlock {
public:
	ContinuousBlock(BufferManager &buffer_manager, const SortingState &sorting_state, const PayloadState &payload_state)
	    : block_idx(0), entry_idx(0), buffer_manager(buffer_manager), sorting_state(sorting_state),
	      payload_state(payload_state) {
		for (idx_t col_idx = 0; col_idx < sorting_state.CONSTANT_SIZE[col_idx]; col_idx++) {
			if (!sorting_state.CONSTANT_SIZE[col_idx]) {
				var_sorting_chunks.push_back(make_unique<ContinuousChunk>(buffer_manager, false));
			} else {
				var_sorting_chunks.push_back(nullptr);
			}
		}
		payload_chunk =
		    make_unique<ContinuousChunk>(buffer_manager, !payload_state.HAS_VARIABLE_SIZE, payload_state.ENTRY_SIZE);
	}

	bool LessThan(ContinuousBlock &other) {
		// TODO: non-constant size columns
		return memcmp(sorting_ptr, other.sorting_ptr, sorting_state.COMP_SIZE) < 0;
	}

	idx_t Count() {
		idx_t count = std::accumulate(sorting_blocks.begin(), sorting_blocks.end(), 0,
		                              [](idx_t a, const RowDataBlock &b) { return a + b.count; });
#ifdef DEBUG
		if (!sorting_state.ALL_CONSTANT) {
			vector<idx_t> counts;
			counts.push_back(count);
			for (idx_t col_idx = 0; col_idx < sorting_state.CONSTANT_SIZE.size(); col_idx++) {
				if (!sorting_state.CONSTANT_SIZE[col_idx]) {
					counts.push_back(var_sorting_chunks[col_idx]->Count());
				}
			}
			counts.push_back(payload_chunk->Count());
			D_ASSERT(std::equal(counts.begin() + 1, counts.end(), counts.begin()));
		}
#endif
		return count;
	}

	idx_t Remaining() {
		idx_t remaining = 0;
		if (Done()) {
			return remaining;
		}
		remaining += sorting_blocks[block_idx].count - entry_idx;
		for (idx_t i = block_idx + 1; i < sorting_blocks.size(); i++) {
			remaining += sorting_blocks[i].count;
		}
		return remaining;
	}

	bool Done() {
		return block_idx >= sorting_blocks.size();
	}

	void InitializeWrite(const OrderGlobalState &state) {
		capacity = state.sorting_block_capacity;
		CreateBlock();

		if (!sorting_state.ALL_CONSTANT) {
			for (idx_t col_idx = 0; col_idx < sorting_state.CONSTANT_SIZE.size(); col_idx++) {
				if (!sorting_state.CONSTANT_SIZE[col_idx]) {
					var_sorting_chunks[col_idx]->InitializeWrite(state.var_sorting_data_block_dims[col_idx],
					                                             state.var_sorting_offset_block_capacity[col_idx]);
				}
			}
		}

		payload_chunk->InitializeWrite(state.payload_data_block_dims, state.payload_offset_block_capacity);
	}

	void CreateBlock() {
		sorting_blocks.emplace_back(buffer_manager, capacity, sorting_state.ENTRY_SIZE);
	}

public:
	//! Memcmp-able representation of sorting columns
	vector<RowDataBlock> sorting_blocks;
	idx_t block_idx;
	idx_t entry_idx;

	//! Variable size sorting columns
	vector<unique_ptr<ContinuousChunk>> var_sorting_chunks;

	//! Payload columns and their offsets
	unique_ptr<ContinuousChunk> payload_chunk;

private:
	//! Buffer manager, and sorting state constants
	BufferManager &buffer_manager;
	const SortingState &sorting_state;
	const PayloadState &payload_state;

	//! Handle and ptr for sorting_blocks
	unique_ptr<BufferHandle> sorting_handle;
	data_ptr_t sorting_ptr;

	idx_t capacity;
};

static void ComputeCountAndCapacity(RowChunk &row_chunk, bool variable_entry_size, idx_t &count, idx_t &capacity) {
	const idx_t &entry_size = row_chunk.entry_size;
	count = 0;
	idx_t total_size = 0;
	for (const auto &block : row_chunk.blocks) {
		count += block.count;
		if (variable_entry_size) {
			total_size += block.byte_offset;
		} else {
			total_size += block.count * entry_size;
		}
	}

	if (variable_entry_size) {
		capacity = MaxValue(Storage::BLOCK_ALLOC_SIZE / entry_size, total_size / entry_size + 1);
	} else {
		capacity = MaxValue(Storage::BLOCK_ALLOC_SIZE / entry_size + 1, count);
	}
}

static RowDataBlock ConcatenateBlocks(BufferManager &buffer_manager, RowChunk &row_chunk, bool variable_entry_size) {
	idx_t total_count;
	idx_t capacity;
	ComputeCountAndCapacity(row_chunk, variable_entry_size, total_count, capacity);
	const idx_t &entry_size = row_chunk.entry_size;

	RowDataBlock new_block(buffer_manager, capacity, entry_size);
	new_block.count = total_count;
	auto new_block_handle = buffer_manager.Pin(new_block.block);
	data_ptr_t new_block_ptr = new_block_handle->node->buffer;

	for (auto &block : row_chunk.blocks) {
		auto block_handle = buffer_manager.Pin(block.block);
		if (variable_entry_size) {
			memcpy(new_block_ptr, block_handle->node->buffer, block.byte_offset);
			new_block_ptr += block.byte_offset;
		} else {
			memcpy(new_block_ptr, block_handle->node->buffer, block.count * entry_size);
			new_block_ptr += block.count * entry_size;
		}
		buffer_manager.UnregisterBlock(block.block->BlockId(), true);
	}
	row_chunk.blocks.clear();
	row_chunk.count = 0;
	return new_block;
}

static RowDataBlock SizesToOffsets(BufferManager &buffer_manager, RowChunk &row_chunk) {
	idx_t total_count;
	idx_t capacity;
	ComputeCountAndCapacity(row_chunk, false, total_count, capacity);

	const idx_t &entry_size = row_chunk.entry_size;
	RowDataBlock new_block(buffer_manager, capacity, entry_size);
	new_block.count = total_count;
	auto new_block_handle = buffer_manager.Pin(new_block.block);
	data_ptr_t new_block_ptr = new_block_handle->node->buffer;
	for (auto &block : row_chunk.blocks) {
		auto block_handle = buffer_manager.Pin(block.block);
		memcpy(new_block_ptr, block_handle->node->buffer, block.count * entry_size);
		new_block_ptr += block.count * entry_size;
		buffer_manager.UnregisterBlock(block.block->BlockId(), true);
	}
	row_chunk.blocks.clear();
	row_chunk.count = 0;
	// convert sizes to offsets
	idx_t *offsets = (idx_t *)new_block_handle->node->buffer;
	idx_t prev = offsets[0];
	offsets[0] = 0;
	idx_t curr;
	for (idx_t i = 1; i < total_count; i++) {
		curr = offsets[i];
		offsets[i] = offsets[i - 1] + prev;
		prev = curr;
	}
	offsets[total_count] = offsets[total_count - 1] + prev;
	return new_block;
}

static bool CompareStrings(const data_ptr_t &l, const data_ptr_t &r, const data_ptr_t &var_dataptr,
                           const idx_t offsets[], const int &order, const idx_t &sorting_size) {
	// use indices to find strings in blob
	idx_t left_idx = Load<idx_t>(l + sorting_size);
	idx_t right_idx = Load<idx_t>(r + sorting_size);
	data_ptr_t left_ptr = var_dataptr + offsets[left_idx];
	data_ptr_t right_ptr = var_dataptr + offsets[right_idx];
	// read string lengths
	uint32_t left_size = Load<uint32_t>(left_ptr);
	uint32_t right_size = Load<uint32_t>(right_ptr);
	left_ptr += string_t::PREFIX_LENGTH;
	right_ptr += string_t::PREFIX_LENGTH;
	// construct strings
	string_t left_val((const char *)left_ptr, left_size);
	string_t right_val((const char *)right_ptr, right_size);

	int comp_res = 1;
	if (Equals::Operation<string_t>(left_val, right_val)) {
		comp_res = 0;
	}
	if (LessThan::Operation<string_t>(left_val, right_val)) {
		comp_res = -1;
	}
	return order * comp_res < 0;
}

static void BreakStringTies(BufferManager &buffer_manager, const data_ptr_t dataptr, const idx_t &start,
                            const idx_t &end, const idx_t &tie_col, bool ties[], const data_ptr_t var_dataptr,
                            const data_ptr_t offsets_ptr, const SortingState &sorting_state) {
	idx_t tie_col_offset = 0;
	for (idx_t i = 0; i < tie_col; i++) {
		tie_col_offset += sorting_state.COL_SIZE[i];
	}
	if (sorting_state.HAS_NULL[tie_col]) {
		char *validity = (char *)dataptr + start * sorting_state.ENTRY_SIZE + tie_col_offset;
		if (sorting_state.ORDER_BY_NULL_TYPES[tie_col] == OrderByNullType::NULLS_FIRST && *validity == 0) {
			// NULLS_FIRST, therefore null is encoded as 0 - we can't break null ties
			return;
		} else if (sorting_state.ORDER_BY_NULL_TYPES[tie_col] == OrderByNullType::NULLS_LAST && *validity == 1) {
			// NULLS_LAST, therefore null is encoded as 1 - we can't break null ties
			return;
		}
		tie_col_offset++;
	}
	// if the tied strings are smaller than the prefix size, or are NULL, we don't need to break the ties
	char *prefix_chars = (char *)dataptr + start * sorting_state.ENTRY_SIZE + tie_col_offset;
	const char null_char = sorting_state.ORDER_TYPES[tie_col] == OrderType::ASCENDING ? 0 : -1;
	for (idx_t i = 0; i < StringStatistics::MAX_STRING_MINMAX_SIZE; i++) {
		if (prefix_chars[i] == null_char) {
			return;
		}
	}

	// fill pointer array for sorting
	auto ptr_block =
	    buffer_manager.Allocate(MaxValue((end - start) * sizeof(data_ptr_t), (idx_t)Storage::BLOCK_ALLOC_SIZE));
	auto entry_ptrs = (data_ptr_t *)ptr_block->node->buffer;
	for (idx_t i = start; i < end; i++) {
		entry_ptrs[i - start] = dataptr + i * sorting_state.ENTRY_SIZE;
	}

	// slow pointer-based sorting
	const int order = sorting_state.ORDER_TYPES[tie_col] == OrderType::DESCENDING ? -1 : 1;
	const idx_t sorting_size = sorting_state.COMP_SIZE;
	const idx_t *offsets = (idx_t *)offsets_ptr;
	std::sort(entry_ptrs, entry_ptrs + end - start,
	          [&var_dataptr, &offsets, &order, &sorting_size](const data_ptr_t l, const data_ptr_t r) {
		          return CompareStrings(l, r, var_dataptr, offsets, order, sorting_size);
	          });

	// re-order
	auto temp_block =
	    buffer_manager.Allocate(MaxValue((end - start) * sorting_state.ENTRY_SIZE, (idx_t)Storage::BLOCK_ALLOC_SIZE));
	data_ptr_t temp_ptr = temp_block->node->buffer;
	for (idx_t i = 0; i < end - start; i++) {
		memcpy(temp_ptr, entry_ptrs[i], sorting_state.ENTRY_SIZE);
		temp_ptr += sorting_state.ENTRY_SIZE;
	}
	memcpy(dataptr + start * sorting_state.ENTRY_SIZE, temp_block->node->buffer,
	       (end - start) * sorting_state.ENTRY_SIZE);

	// determine if there are still ties (if this is not the last column)
	if (tie_col < sorting_state.ORDER_TYPES.size() - 1) {
		data_ptr_t idx_ptr = dataptr + start * sorting_state.ENTRY_SIZE + sorting_size;

		idx_t current_idx = Load<idx_t>(idx_ptr);
		data_ptr_t current_ptr = var_dataptr + offsets[current_idx];
		uint32_t current_size = Load<uint32_t>(current_ptr);
		current_ptr += string_t::PREFIX_LENGTH;
		string_t current_val((const char *)current_ptr, current_size);
		for (idx_t i = 0; i < end - start - 1; i++) {
			idx_ptr += sorting_state.ENTRY_SIZE;

			// load next entry
			idx_t next_idx = Load<idx_t>(idx_ptr);
			data_ptr_t next_ptr = var_dataptr + offsets[next_idx];
			uint32_t next_size = Load<uint32_t>(next_ptr);
			next_ptr += string_t::PREFIX_LENGTH;
			string_t next_val((const char *)next_ptr, next_size);

			if (current_size != next_size) {
				// quick comparison: different length
				ties[start + i] = false;
			} else {
				// equal length: full comparison
				ties[start + i] = Equals::Operation<string_t>(current_val, next_val);
			}

			current_size = next_size;
			current_val = next_val;
		}
	}
}

static void BreakTies(BufferManager &buffer_manager, ContinuousBlock &cb, bool ties[], data_ptr_t dataptr,
                      const idx_t &count, const idx_t &tie_col, const SortingState &sorting_state) {
	D_ASSERT(!ties[count - 1]);
	auto &var_data_block = cb.var_sorting_chunks[tie_col]->data_blocks.back();
	auto &var_offsets_block = cb.var_sorting_chunks[tie_col]->offset_blocks.back();
	auto var_block_handle = buffer_manager.Pin(var_data_block.block);
	auto var_sizes_handle = buffer_manager.Pin(var_offsets_block.block);
	const data_ptr_t var_dataptr = var_block_handle->node->buffer;
	const data_ptr_t offsets_ptr = var_sizes_handle->node->buffer;

	for (idx_t i = 0; i < count; i++) {
		if (!ties[i]) {
			continue;
		}
		idx_t j;
		for (j = i; j < count; j++) {
			if (!ties[j]) {
				break;
			}
		}
		switch (sorting_state.TYPES[tie_col].InternalType()) {
		case PhysicalType::VARCHAR:
			BreakStringTies(buffer_manager, dataptr, i, j + 1, tie_col, ties, var_dataptr, offsets_ptr, sorting_state);
			break;
		default:
			throw NotImplementedException("Cannot sort variable size column with type %s",
			                              sorting_state.TYPES[tie_col].ToString());
		}
		i = j;
	}
}

static bool AnyTies(bool ties[], const idx_t &count) {
	D_ASSERT(!ties[count - 1]);
	bool any_ties = false;
	for (idx_t i = 0; i < count - 1; i++) {
		any_ties = any_ties || ties[i];
	}
	return any_ties;
}

static void ComputeTies(data_ptr_t dataptr, const idx_t &count, const idx_t &col_offset, const idx_t &tie_size,
                        bool ties[], const SortingState &sorting_state) {
	D_ASSERT(!ties[count - 1]);
	D_ASSERT(col_offset + tie_size <= sorting_state.COMP_SIZE);
	// align dataptr
	dataptr += col_offset;
	idx_t i = 0;
	for (; i + 7 < count - 1; i += 8) {
		// fixed size inner loop to allow unrolling
		for (idx_t j = 0; j < 8; j++) {
			ties[i + j] = ties[i + j] && memcmp(dataptr, dataptr + sorting_state.ENTRY_SIZE, tie_size) == 0;
			dataptr += sorting_state.ENTRY_SIZE;
		}
	}
	for (; i < count - 1; i++) {
		ties[i] = ties[i] && memcmp(dataptr, dataptr + sorting_state.ENTRY_SIZE, tie_size) == 0;
		dataptr += sorting_state.ENTRY_SIZE;
	}
	ties[count - 1] = false;
}

static void RadixSort(BufferManager &buffer_manager, data_ptr_t dataptr, const idx_t &count, const idx_t &col_offset,
                      const idx_t &sorting_size, const SortingState &sorting_state) {
	auto temp_block =
	    buffer_manager.Allocate(MaxValue(count * sorting_state.ENTRY_SIZE, (idx_t)Storage::BLOCK_ALLOC_SIZE));
	data_ptr_t temp = temp_block->node->buffer;
	bool swap = false;

	idx_t counts[256];
	uint8_t byte;
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
		for (idx_t i = count; i > 0; i--) {
			byte = *(dataptr + (i - 1) * sorting_state.ENTRY_SIZE + offset);
			memcpy(temp + (counts[byte] - 1) * sorting_state.ENTRY_SIZE, dataptr + (i - 1) * sorting_state.ENTRY_SIZE,
			       sorting_state.ENTRY_SIZE);
			counts[byte]--;
		}
		std::swap(dataptr, temp);
		swap = !swap;
	}
	// move data back to original buffer (if it was swapped)
	if (swap) {
		memcpy(temp, dataptr, count * sorting_state.ENTRY_SIZE);
	}
}

static void SubSortTiedTuples(BufferManager &buffer_manager, const data_ptr_t dataptr, const idx_t &count,
                              const idx_t &col_offset, const idx_t &sorting_size, bool ties[],
                              const SortingState &sorting_state) {
	D_ASSERT(!ties[count - 1]);
	for (idx_t i = 0; i < count; i++) {
		if (!ties[i]) {
			continue;
		}
		idx_t j;
		for (j = i + 1; j < count; j++) {
			if (!ties[j]) {
				break;
			}
		}
		RadixSort(buffer_manager, dataptr + i * sorting_state.ENTRY_SIZE, j - i + 1, col_offset, sorting_size,
		          sorting_state);
		i = j;
	}
}

static void SortInMemory(BufferManager &buffer_manager, ContinuousBlock &cb, const SortingState &sorting_state) {
	auto &block = cb.sorting_blocks.back();
	const auto &count = block.count;
	auto handle = buffer_manager.Pin(block.block);
	const auto dataptr = handle->node->buffer;

	// assign an index to each row
	data_ptr_t idx_dataptr = dataptr + sorting_state.COMP_SIZE;
	for (idx_t i = 0; i < count; i++) {
		Store<idx_t>(i, idx_dataptr);
		idx_dataptr += sorting_state.ENTRY_SIZE;
	}

	bool all_constant = true;
	for (idx_t i = 0; i < sorting_state.CONSTANT_SIZE.size(); i++) {
		all_constant = all_constant && sorting_state.CONSTANT_SIZE[i];
	}

	if (all_constant) {
		RadixSort(buffer_manager, dataptr, count, 0, sorting_state.COMP_SIZE, sorting_state);
		return;
	}

	idx_t sorting_size = 0;
	idx_t col_offset = 0;
	unique_ptr<BufferHandle> ties_handle = nullptr;
	bool *ties = nullptr;
	const idx_t num_cols = sorting_state.CONSTANT_SIZE.size();
	for (idx_t i = 0; i < num_cols; i++) {
		sorting_size += sorting_state.COL_SIZE[i];
		if (sorting_state.CONSTANT_SIZE[i] && i < num_cols - 1) {
			// add columns to the sort until we reach a variable size column, or the last column
			continue;
		}

		if (!ties) {
			// this is the first sort
			RadixSort(buffer_manager, dataptr, count, col_offset, sorting_size, sorting_state);
			ties_handle = buffer_manager.Allocate(MaxValue(count, (idx_t)Storage::BLOCK_ALLOC_SIZE));
			ties = (bool *)ties_handle->node->buffer;
			std::fill_n(ties, count - 1, true);
			ties[count - 1] = false;
		} else {
			// for subsequent sorts, we subsort the tied tuples
			SubSortTiedTuples(buffer_manager, dataptr, count, col_offset, sorting_size, ties, sorting_state);
		}

		if (sorting_state.CONSTANT_SIZE[i] && i == num_cols - 1) {
			// all columns are sorted, no ties to break because last column is constant size
			break;
		}

		ComputeTies(dataptr, count, col_offset, sorting_size, ties, sorting_state);
		if (!AnyTies(ties, count)) {
			// no ties, so we stop sorting
			break;
		}

		BreakTies(buffer_manager, cb, ties, dataptr, count, i, sorting_state);
		if (!AnyTies(ties, count)) {
			// no more ties after tie-breaking
			break;
		}

		col_offset += sorting_size;
		sorting_size = 0;
	}
}

static void ReOrder(BufferManager &buffer_manager, ContinuousChunk &cc, data_ptr_t sorting_ptr,
                    const SortingState &sorting_state) {
	const idx_t &count = cc.data_blocks.back().count;

	auto &unordered_data_block = cc.data_blocks.back();
	auto unordered_data_handle = buffer_manager.Pin(unordered_data_block.block);
	const data_ptr_t unordered_data_ptr = unordered_data_handle->node->buffer;

	RowDataBlock reordered_data_block(buffer_manager, unordered_data_block.CAPACITY, unordered_data_block.ENTRY_SIZE);
	reordered_data_block.count = count;
	auto ordered_data_handle = buffer_manager.Pin(reordered_data_block.block);
	data_ptr_t ordered_data_ptr = ordered_data_handle->node->buffer;

	if (cc.CONSTANT_SIZE) {
		const idx_t entry_size = unordered_data_block.ENTRY_SIZE;
		for (idx_t i = 0; i < count; i++) {
			idx_t index = Load<idx_t>(sorting_ptr);
			memcpy(ordered_data_ptr, unordered_data_ptr + index * entry_size, entry_size);
			ordered_data_ptr += entry_size;
			sorting_ptr += sorting_state.ENTRY_SIZE;
		}
	} else {
		// variable size data: we need offsets too
		reordered_data_block.byte_offset = unordered_data_block.byte_offset;
		auto &unordered_offset_block = cc.offset_blocks.back();
		auto unordered_offset_handle = buffer_manager.Pin(unordered_offset_block.block);
		idx_t *unordered_offsets = (idx_t *)unordered_offset_handle->node->buffer;

		RowDataBlock reordered_offset_block(buffer_manager, unordered_offset_block.CAPACITY,
		                                    unordered_offset_block.ENTRY_SIZE);
		reordered_offset_block.count = count;
		auto reordered_offset_handle = buffer_manager.Pin(reordered_offset_block.block);
		idx_t *reordered_offsets = (idx_t *)reordered_offset_handle->node->buffer;
		reordered_offsets[0] = 0;

		for (idx_t i = 0; i < count; i++) {
			idx_t index = Load<idx_t>(sorting_ptr);
			idx_t size = unordered_offsets[index + 1] - unordered_offsets[index];
			memcpy(ordered_data_ptr, unordered_data_ptr + unordered_offsets[index], size);
			ordered_data_ptr += size;
			reordered_offsets[i + 1] = reordered_offsets[i] + size;
			sorting_ptr += sorting_state.ENTRY_SIZE;
		}
		// replace offset block
		buffer_manager.UnregisterBlock(unordered_offset_block.block->BlockId(), true);
		cc.offset_blocks.clear();
		cc.offset_blocks.push_back(move(reordered_offset_block));
	}
	// replace data block
	buffer_manager.UnregisterBlock(unordered_data_block.block->BlockId(), true);
	cc.data_blocks.clear();
	cc.data_blocks.push_back(move(reordered_data_block));
}

//! Use the ordered sorting data to re-order the rest of the data
static void ReOrder(ClientContext &context, ContinuousBlock &cb, const SortingState &sorting_state,
                    const PayloadState &payload_state) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto sorting_handle = buffer_manager.Pin(cb.sorting_blocks.back().block);
	const data_ptr_t sorting_ptr = sorting_handle->node->buffer + sorting_state.COMP_SIZE;

	// re-order variable size sorting columns
	for (idx_t col_idx = 0; col_idx < sorting_state.CONSTANT_SIZE.size(); col_idx++) {
		if (!sorting_state.CONSTANT_SIZE[col_idx]) {
			ReOrder(buffer_manager, *cb.var_sorting_chunks[col_idx], sorting_ptr, sorting_state);
		}
	}
	// and the payload
	ReOrder(buffer_manager, *cb.payload_chunk, sorting_ptr, sorting_state);
}

void PhysicalOrder::SortLocalState(ClientContext &context, OrderLocalState &lstate, const SortingState &sorting_state,
                                   const PayloadState &payload_state) {
	const idx_t &count = lstate.sorting_block->count;
	D_ASSERT(count == lstate.payload_block->count);
	if (lstate.sorting_block->count == 0) {
		return;
	}

	// copy all data to ContinuousBlocks
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto cb = make_unique<ContinuousBlock>(buffer_manager, sorting_state, payload_state);
	// fixed-size sorting data
	auto sorting_block = ConcatenateBlocks(buffer_manager, *lstate.sorting_block, false);
	cb->sorting_blocks.push_back(move(sorting_block));
	// variable size sorting columns
	for (idx_t i = 0; i < lstate.var_sorting_blocks.size(); i++) {
		if (!sorting_state.CONSTANT_SIZE[i]) {
			auto &cc = cb->var_sorting_chunks[i];
			auto &row_chunk = *lstate.var_sorting_blocks[i];
			auto new_block = ConcatenateBlocks(buffer_manager, row_chunk, true);
			auto &sizes_chunk = *lstate.var_sorting_sizes[i];
			auto offsets_block = SizesToOffsets(buffer_manager, sizes_chunk);
			cc->data_blocks.push_back(move(new_block));
			cc->offset_blocks.push_back(move(offsets_block));
		}
	}
	// payload data
	auto payload_block = ConcatenateBlocks(buffer_manager, *lstate.payload_block, payload_state.HAS_VARIABLE_SIZE);
	cb->payload_chunk->data_blocks.push_back(move(payload_block));
	if (payload_state.HAS_VARIABLE_SIZE) {
		auto offsets_block = SizesToOffsets(buffer_manager, *lstate.sizes_block);
		cb->payload_chunk->offset_blocks.push_back(move(offsets_block));
	}

	// now perform the actual sort
	SortInMemory(buffer_manager, *cb, sorting_state);

	// re-order before the merge sort
	ReOrder(context, *cb, sorting_state, payload_state);

	// add the sorted block to the global state
	lstate.sorted_blocks.push_back(move(cb));
}

class PhysicalOrderMergeTask : public Task {
public:
	PhysicalOrderMergeTask(Pipeline &parent_p, ClientContext &context_p, OrderGlobalState &state_p,
	                       ContinuousBlock &left_p, ContinuousBlock &right_p)
	    : parent(parent_p), context(context_p), buffer_manager(BufferManager::GetBufferManager(context_p)),
	      state(state_p), sorting_state(*state_p.sorting_state), payload_state(*state_p.payload_state), left(left_p),
	      right(right_p) {
		result = make_unique<ContinuousBlock>(buffer_manager, sorting_state, payload_state);
	}

	void Execute() override {
		result->InitializeWrite(state);
		bool left_smaller[PhysicalOrder::MERGE_STRIDE];
		while (true) {
			auto l_remaining = left.Remaining();
			auto r_remaining = right.Remaining();
			if (l_remaining + r_remaining == 0) {
				break;
			}
			const idx_t next = MinValue(l_remaining + r_remaining, PhysicalOrder::MERGE_STRIDE);
			ComputeNextMerge(next, left_smaller);
			MergeNext(next, left_smaller);
			if (!sorting_state.ALL_CONSTANT) {
				for (idx_t col_idx = 0; col_idx < sorting_state.CONSTANT_SIZE.size(); col_idx++) {
					if (!sorting_state.CONSTANT_SIZE[col_idx]) {
						MergeNext(*result->var_sorting_chunks[col_idx], *left.var_sorting_chunks[col_idx],
						          *right.var_sorting_chunks[col_idx], next, left_smaller);
					}
				}
			}
			MergeNext(*result->payload_chunk, *left.payload_chunk, *right.payload_chunk, next, left_smaller);
		}
		D_ASSERT(result->Count() == left.Count() + right.Count());

		lock_guard<mutex> glock(state.lock);
		state.sorted_blocks_temp.push_back(move(result));
		parent.finished_tasks++;
		if (parent.total_tasks == parent.finished_tasks) {
			// TODO: cleanup
			state.sorted_blocks.clear();
			for (auto &block : state.sorted_blocks_temp) {
				state.sorted_blocks.push_back(move(block));
			}
			state.sorted_blocks_temp.clear();
			PhysicalOrder::ScheduleMergeTasks(parent, context, state);
		}
	}

	void ComputeNextMerge(const idx_t &count, bool left_smaller[]) {
		idx_t l_block_idx = left.block_idx;
		idx_t r_block_idx = right.block_idx;
		idx_t l_entry_idx = left.entry_idx;
		idx_t r_entry_idx = right.entry_idx;

		RowDataBlock *left_block = nullptr;
		RowDataBlock *right_block = nullptr;
		unique_ptr<BufferHandle> l_block_handle;
		unique_ptr<BufferHandle> r_block_handle;
		data_ptr_t l_ptr;
		data_ptr_t r_ptr;

		idx_t compared = 0;
		while (compared < count) {
			// move to the next block (if needed)
			if (l_block_idx < left.sorting_blocks.size() && l_entry_idx == left.sorting_blocks[l_block_idx].count) {
				l_block_idx++;
				l_entry_idx = 0;
			}
			if (r_block_idx < right.sorting_blocks.size() && r_entry_idx == right.sorting_blocks[r_block_idx].count) {
				r_block_idx++;
				r_entry_idx = 0;
			}
			// pin the blocks
			if (l_block_idx < left.sorting_blocks.size()) {
				left_block = &left.sorting_blocks[l_block_idx];
				l_block_handle = buffer_manager.Pin(left_block->block);
				l_ptr = l_block_handle->node->buffer;
			}
			if (r_block_idx < right.sorting_blocks.size()) {
				right_block = &right.sorting_blocks[r_block_idx];
				r_block_handle = buffer_manager.Pin(right_block->block);
				r_ptr = r_block_handle->node->buffer;
			}
			const idx_t &l_count = left_block ? left_block->count : 0;
			const idx_t &r_count = right_block ? right_block->count : 0;
			// fill left_smaller array and copy sorting data
			if (sorting_state.ALL_CONSTANT) {
				// all sorting columns are constant size
				if (l_block_idx < left.sorting_blocks.size() && r_block_idx < right.sorting_blocks.size()) {
					// neither left or right side is exhausted - compare in loop
					for (; compared < count && l_entry_idx < l_count && r_entry_idx < r_count; compared++) {
						// compare
						const bool l_smaller = memcmp(l_ptr, r_ptr, sorting_state.COMP_SIZE) < 0;
						left_smaller[compared] = l_smaller;
						// use comparison bool (0 or 1) to increment entries and pointers
						l_entry_idx += l_smaller;
						r_entry_idx += one - l_smaller;
						l_ptr += l_smaller * sorting_state.ENTRY_SIZE;
						r_ptr += (one - l_smaller) * sorting_state.ENTRY_SIZE;
					}
				} else if (r_block_idx == right.sorting_blocks.size()) {
					// right side is exhausted
					std::fill_n(left_smaller + compared, count - compared, true);
					compared = count;
				} else {
					// left side is exhausted
					std::fill_n(left_smaller + compared, count - compared, false);
					compared = count;
				}
			} else {
				// TODO: variable size stuff
			}
		}
	}

	void MergeNext(const idx_t &count, const bool left_smaller[]) {
		idx_t l_block_idx = left.block_idx;
		idx_t l_entry_idx = left.entry_idx;
		idx_t r_block_idx = right.block_idx;
		idx_t r_entry_idx = right.entry_idx;

		RowDataBlock *left_block = nullptr;
		RowDataBlock *right_block = nullptr;
		unique_ptr<BufferHandle> l_block_handle;
		unique_ptr<BufferHandle> r_block_handle;
		data_ptr_t l_ptr;
		data_ptr_t r_ptr;

		RowDataBlock *result_block = &result->sorting_blocks.back();
		auto result_handle = buffer_manager.Pin(result_block->block);
		data_ptr_t result_ptr = result_handle->node->buffer;

		idx_t copied = 0;
		while (copied < count) {
			// move to the next block (if needed)
			if (l_block_idx < left.sorting_blocks.size() && l_entry_idx == left.sorting_blocks[l_block_idx].count) {
				l_block_idx++;
				l_entry_idx = 0;
			}
			if (r_block_idx < right.sorting_blocks.size() && r_entry_idx == right.sorting_blocks[r_block_idx].count) {
				r_block_idx++;
				r_entry_idx = 0;
			}
			// pin the blocks
			if (l_block_idx < left.sorting_blocks.size()) {
				left_block = &left.sorting_blocks[l_block_idx];
				l_block_handle = buffer_manager.Pin(left_block->block);
				l_ptr = l_block_handle->node->buffer;
			}
			if (r_block_idx < right.sorting_blocks.size()) {
				right_block = &right.sorting_blocks[r_block_idx];
				r_block_handle = buffer_manager.Pin(right_block->block);
				r_ptr = r_block_handle->node->buffer;
			}
			const idx_t &l_count = left_block ? left_block->count : 0;
			const idx_t &r_count = right_block ? right_block->count : 0;
			// create new result block (if needed)
			if (result_block->count == result_block->CAPACITY) {
				result->CreateBlock();
				result_block = &result->sorting_blocks.back();
				result_handle = buffer_manager.Pin(result_block->block);
				result_ptr = result_handle->node->buffer;
			}
			const idx_t next = MinValue(count - copied, result_block->CAPACITY - result_block->count);
			// copy using computed merge
			idx_t i;
			if (l_block_idx < left.sorting_blocks.size() && r_block_idx < right.sorting_blocks.size()) {
				// neither left or right side is exhausted
				for (i = 0; i < next && l_entry_idx < l_count && r_entry_idx < r_count; i++) {
					const bool l_smaller = left_smaller[copied + i];
					// use comparison bool (0 or 1) to copy an entry from either side
					memcpy(result_ptr, l_ptr, l_smaller * sorting_state.ENTRY_SIZE);
					memcpy(result_ptr, r_ptr, (one - l_smaller) * sorting_state.ENTRY_SIZE);
					result_ptr += sorting_state.ENTRY_SIZE;
					result_block->count++;
					// use the comparison bool to increment entries and pointers
					l_entry_idx += l_smaller;
					r_entry_idx += one - l_smaller;
					l_ptr += l_smaller * sorting_state.COMP_SIZE;
					r_ptr += (one - l_smaller) * sorting_state.COMP_SIZE;
				}
			} else if (r_block_idx == right.sorting_blocks.size()) {
				// right side is exhausted
				const idx_t l_next = MinValue(next, l_count - l_entry_idx);
				for (i = 0; i < l_next; i++) {
					memcpy(result_ptr, l_ptr, sorting_state.ENTRY_SIZE);
					result_ptr += sorting_state.ENTRY_SIZE;
					result_block->count++;
					l_ptr += sorting_state.ENTRY_SIZE;
					l_entry_idx++;
				}
			} else {
				// left side is exhausted
				const idx_t r_next = MinValue(next, r_count - r_entry_idx);
				for (i = 0; i < r_next; i++) {
					memcpy(result_ptr, r_ptr, sorting_state.ENTRY_SIZE);
					result_ptr += sorting_state.ENTRY_SIZE;
					result_block->count++;
					r_ptr += sorting_state.ENTRY_SIZE;
					r_entry_idx++;
				}
			}
			copied += i;
		}

		left.block_idx = l_block_idx;
		left.entry_idx = l_entry_idx;
		right.block_idx = r_block_idx;
		right.entry_idx = r_entry_idx;
	}

	void MergeNext(ContinuousChunk &result_cc, ContinuousChunk &left_cc, ContinuousChunk &right_cc, const idx_t &count,
	               const bool left_smaller[]) {
		idx_t l_block_idx = left_cc.data_block_idx;
		idx_t l_entry_idx = left_cc.data_entry_idx;
		idx_t r_block_idx = right_cc.data_block_idx;
		idx_t r_entry_idx = right_cc.data_entry_idx;

		RowDataBlock *left_block = nullptr;
		RowDataBlock *right_block = nullptr;
		unique_ptr<BufferHandle> l_block_handle;
		unique_ptr<BufferHandle> r_block_handle;
		data_ptr_t l_ptr;
		data_ptr_t r_ptr;

		RowDataBlock *result_block = &result_cc.data_blocks.back();
		auto result_handle = buffer_manager.Pin(result_block->block);
		data_ptr_t result_ptr = result_handle->node->buffer;

		idx_t copied = 0;
		while (copied < count) {
			// move to the next block (if needed)
			if (l_block_idx < left_cc.data_blocks.size() && l_entry_idx == left_cc.data_blocks[l_block_idx].count) {
				l_block_idx++;
				l_entry_idx = 0;
			}
			if (r_block_idx < right_cc.data_blocks.size() && r_entry_idx == right_cc.data_blocks[r_block_idx].count) {
				r_block_idx++;
				r_entry_idx = 0;
			}
			// pin the blocks
			if (l_block_idx < left_cc.data_blocks.size()) {
				left_block = &left_cc.data_blocks[l_block_idx];
				l_block_handle = buffer_manager.Pin(left_block->block);
				l_ptr = l_block_handle->node->buffer;
			}
			if (r_block_idx < right_cc.data_blocks.size()) {
				right_block = &right_cc.data_blocks[r_block_idx];
				r_block_handle = buffer_manager.Pin(right_block->block);
				r_ptr = r_block_handle->node->buffer;
			}
			const idx_t &l_count = left_block ? left_block->count : 0;
			const idx_t &r_count = right_block ? right_block->count : 0;
			// TODO: the copying
			idx_t i;
			if (result_cc.CONSTANT_SIZE) {
				// create new result block (if needed)
				if (result_block->count == result_block->CAPACITY) {
					result_cc.CreateDataBlock();
					result_block = &result_cc.data_blocks.back();
					result_handle = buffer_manager.Pin(result_block->block);
					result_ptr = result_handle->node->buffer;
				}
                const idx_t next = MinValue(count - copied, result_block->CAPACITY - result_block->count);
				const idx_t entry_size = result_cc.ENTRY_SIZE;
				if (l_block_idx < left_cc.data_blocks.size() && r_block_idx < right_cc.data_blocks.size()) {
					// neither left or right side is exhausted
					for (i = 0; i < next && l_entry_idx < l_count && r_entry_idx < r_count; i++) {
						const bool l_smaller = left_smaller[copied + i];
						// use comparison bool (0 or 1) to copy an entry from either side
						memcpy(result_ptr, l_ptr, l_smaller * entry_size);
						memcpy(result_ptr, r_ptr, (one - l_smaller) * entry_size);
						result_ptr += entry_size;
						result_block->count++;
						// use the comparison bool to increment entries and pointers
						l_entry_idx += l_smaller;
						r_entry_idx += one - l_smaller;
						l_ptr += l_smaller * entry_size;
						r_ptr += (one - l_smaller) * entry_size;
					}
				} else if (r_block_idx == right_cc.data_blocks.size()) {
					// right side is exhausted
					const idx_t l_next = MinValue(next, l_count - l_entry_idx);
					for (i = 0; i < l_next; i++) {
						memcpy(result_ptr, l_ptr, entry_size);
						result_ptr += entry_size;
						result_block->count++;
						l_ptr += entry_size;
						l_entry_idx++;
					}
				} else {
					// left side is exhausted
					const idx_t r_next = MinValue(next, r_count - r_entry_idx);
					for (i = 0; i < r_next; i++) {
						memcpy(result_ptr, r_ptr, entry_size);
						result_ptr += entry_size;
						result_block->count++;
						r_ptr += entry_size;
						r_entry_idx++;
					}
				}
			} else {
				// TODO: init new block
				i = 0;
			}
			copied += i;
		}
	}

private:
	Pipeline &parent;
	ClientContext &context;
	BufferManager &buffer_manager;
	OrderGlobalState &state;
	const SortingState &sorting_state;
	const PayloadState &payload_state;

	ContinuousBlock &left;
	ContinuousBlock &right;
	unique_ptr<ContinuousBlock> result;

	const uint8_t one = 1;
};

void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state_p) {
	this->sink_state = move(state_p);
	auto &state = (OrderGlobalState &)*this->sink_state;

	if (state.sorted_blocks.empty()) {
		return;
	}

	idx_t count = 0;
	idx_t sorting_block_capacity = 0;
	for (auto &sb : state.sorted_blocks) {
		const auto &sorting_block = sb->sorting_blocks.back();
		count += sorting_block.count;
		if (sorting_block.CAPACITY > sorting_block_capacity) {
			sorting_block_capacity = sorting_block.CAPACITY;
		}
	}
	state.total_count = count;
	state.sorting_block_capacity = sorting_block_capacity;

	const auto &sorting_state = *state.sorting_state;
	if (!sorting_state.ALL_CONSTANT) {
		for (idx_t col_idx = 0; col_idx < sorting_state.CONSTANT_SIZE.size(); col_idx++) {
			idx_t var_data_capacity = 0;
			idx_t var_data_entry_size = 0;
			idx_t var_offset_capacity = 0;
			if (sorting_state.CONSTANT_SIZE[col_idx]) {
				for (auto &sb : state.sorted_blocks) {
					const auto &data_block = sb->var_sorting_chunks[col_idx]->data_blocks.back();
					if (data_block.CAPACITY > var_data_capacity) {
						var_data_capacity = data_block.CAPACITY;
					}
					if (data_block.ENTRY_SIZE > var_data_entry_size) {
						var_data_entry_size = data_block.ENTRY_SIZE;
					}
					const auto &offset_block = sb->var_sorting_chunks[col_idx]->offset_blocks.back();
					if (offset_block.CAPACITY > var_offset_capacity) {
						var_offset_capacity = offset_block.CAPACITY;
					}
				}
			}
			state.var_sorting_data_block_dims.emplace_back(var_data_capacity, var_data_entry_size);
			state.var_sorting_offset_block_capacity.push_back(var_offset_capacity);
		}
	}

    const auto &payload_state = *state.payload_state;
	idx_t payload_capacity = 0;
	idx_t payload_entry_size = 0;
	idx_t payload_offset_capacity = 0;
	for (auto &sb : state.sorted_blocks) {
		auto &data_block = sb->payload_chunk->data_blocks.back();
		if (data_block.CAPACITY > payload_capacity) {
			payload_capacity = data_block.CAPACITY;
		}
		if (data_block.ENTRY_SIZE > payload_entry_size) {
			payload_entry_size = data_block.ENTRY_SIZE;
		}
		if (payload_state.HAS_VARIABLE_SIZE) {
			auto &offset_block = sb->payload_chunk->offset_blocks.back();
			if (offset_block.CAPACITY > payload_offset_capacity) {
				payload_capacity = offset_block.CAPACITY;
			}
		}
	}
	state.payload_data_block_dims = make_pair(payload_capacity, payload_entry_size);
	state.payload_offset_block_capacity = payload_offset_capacity;

	if (state.sorted_blocks.size() > 1) {
		PhysicalOrder::ScheduleMergeTasks(pipeline, context, state);
	}
	// TODO: else cleanup
}

void PhysicalOrder::ScheduleMergeTasks(Pipeline &pipeline, ClientContext &context, OrderGlobalState &state) {
	if (state.sorted_blocks.size() == 1) {
		// TODO: cleanup
		pipeline.Finish();
		return;
	}

	std::random_shuffle(state.sorted_blocks.begin(), state.sorted_blocks.end());
	if (state.sorted_blocks.size() % 2 == 1) {
		state.sorted_blocks_temp.push_back(move(state.sorted_blocks.back()));
		state.sorted_blocks.pop_back();
	}

	auto &ts = TaskScheduler::GetScheduler(context);
	for (idx_t i = 0; i < state.sorted_blocks.size(); i += 2) {
		auto new_task = make_unique<PhysicalOrderMergeTask>(pipeline, context, state, *state.sorted_blocks[i],
		                                                    *state.sorted_blocks[i + 1]);
		pipeline.total_tasks++;
		ts.ScheduleTask(pipeline.token, move(new_task));
	}
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
idx_t PhysicalOrder::MaxThreads(ClientContext &context) {
	if (this->sink_state) {
		auto &state = (OrderGlobalState &)*this->sink_state;
		return state.total_count / STANDARD_VECTOR_SIZE + 1;
	} else {
		return estimated_cardinality / STANDARD_VECTOR_SIZE + 1;
	}
}

class OrderParallelState : public ParallelState {
public:
	OrderParallelState()
	    : global_entry_idx(0), data_block_idx(0), data_entry_idx(0), offset_block_idx(0), offset_entry_idx(0) {
	}

	idx_t global_entry_idx;

	idx_t data_block_idx;
	idx_t data_entry_idx;

	idx_t offset_block_idx;
	idx_t offset_entry_idx;

	std::mutex lock;
};

unique_ptr<ParallelState> PhysicalOrder::GetParallelState() {
	auto result = make_unique<OrderParallelState>();
	return move(result);
}

class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderOperatorState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), initialized(false), global_entry_idx(0), data_block_idx(0),
	      data_entry_idx(0), offset_block_idx(0), offset_entry_idx(0) {
	}
	bool initialized;
	ParallelState *parallel_state;

	ContinuousChunk *payload_chunk;

	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t validitymask_locations[STANDARD_VECTOR_SIZE];

	idx_t global_entry_idx;

	idx_t data_block_idx;
	idx_t data_entry_idx;

	idx_t offset_block_idx;
	idx_t offset_entry_idx;

	void InitNextIndices() {
		auto &p_state = *reinterpret_cast<OrderParallelState *>(parallel_state);
		global_entry_idx = p_state.global_entry_idx;
		data_block_idx = p_state.data_block_idx;
		data_entry_idx = p_state.data_entry_idx;
		offset_block_idx = p_state.offset_block_idx;
		offset_entry_idx = p_state.offset_entry_idx;
	}
};

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(*this, children[0].get());
}

static void UpdateStateIndices(const idx_t &next, const ContinuousChunk &payload_chunk,
                               OrderParallelState &parallel_state) {
	parallel_state.global_entry_idx += next;

	idx_t remaining = next;
	while (remaining > 0) {
		auto &data_block = payload_chunk.data_blocks[parallel_state.data_block_idx];
		if (parallel_state.data_entry_idx + remaining > data_block.count) {
			remaining -= data_block.count - parallel_state.data_entry_idx;
			parallel_state.data_block_idx++;
			parallel_state.data_entry_idx = 0;
		} else {
			parallel_state.data_entry_idx += remaining;
			remaining = 0;
		}
	}

	if (!payload_chunk.CONSTANT_SIZE) {
		remaining = next;
		while (remaining > 0) {
			auto &offset_block = payload_chunk.offset_blocks[parallel_state.offset_block_idx];
			if (parallel_state.offset_entry_idx + remaining > offset_block.count) {
				remaining -= offset_block.count - parallel_state.offset_entry_idx;
				parallel_state.offset_block_idx++;
				parallel_state.offset_entry_idx = 0;
			} else {
				parallel_state.offset_entry_idx += remaining;
				remaining = 0;
			}
		}
	}
}

static void Scan(ClientContext &context, DataChunk &chunk, PhysicalOrderOperatorState &state,
                 const PayloadState &payload_state, const idx_t &scan_count) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto &payload_chunk = *state.payload_chunk;

	vector<unique_ptr<BufferHandle>> handles;

	// set up a batch of pointers to scan data from
	idx_t count = 0;
	while (count < scan_count) {
		auto &data_block = payload_chunk.data_blocks[state.data_block_idx];
		auto data_handle = buffer_manager.Pin(data_block.block);
		idx_t next = MinValue(data_block.count - state.data_entry_idx, scan_count - count);
		if (payload_state.HAS_VARIABLE_SIZE) {
			auto &offset_block = payload_chunk.offset_blocks[state.offset_block_idx];
			auto offset_handle = buffer_manager.Pin(offset_block.block);
			next = MinValue(next, offset_block.count - state.offset_entry_idx);

			const data_ptr_t payl_dataptr = data_handle->node->buffer;
			const idx_t *offsets = (idx_t *)offset_handle->node->buffer;
			for (idx_t i = 0; i < next; i++) {
				state.validitymask_locations[count + i] = payl_dataptr + offsets[state.offset_entry_idx + i];
				state.key_locations[count + i] =
				    state.validitymask_locations[count + i] + payload_state.VALIDITYMASK_SIZE;
			}

			state.offset_entry_idx += next;
			if (state.offset_entry_idx == offset_block.count) {
				state.offset_block_idx++;
				state.offset_entry_idx = 0;
			}
		} else {
			data_ptr_t payl_dataptr = data_handle->node->buffer + state.data_entry_idx * payload_state.ENTRY_SIZE;
			for (idx_t i = 0; i < next; i++) {
				state.validitymask_locations[count + i] = payl_dataptr;
				state.key_locations[count + i] = payl_dataptr + payload_state.VALIDITYMASK_SIZE;
				payl_dataptr += payload_state.ENTRY_SIZE;
			}
		}
		state.data_entry_idx += next;
		if (state.data_entry_idx == data_block.count) {
			state.data_block_idx++;
			state.data_entry_idx = 0;
		}
		count += next;
		handles.push_back(move(data_handle));
	}
	D_ASSERT(count == scan_count);
	state.global_entry_idx += scan_count;

	// deserialize the payload data
	for (idx_t payl_col = 0; payl_col < chunk.ColumnCount(); payl_col++) {
		RowChunk::DeserializeIntoVector(chunk.data[payl_col], scan_count, payl_col, state.key_locations,
		                                state.validitymask_locations);
	}
	chunk.SetCardinality(scan_count);
	chunk.Verify();
}

void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto &state = *reinterpret_cast<PhysicalOrderOperatorState *>(state_p);
	auto &gstate = (OrderGlobalState &)*this->sink_state;
	const auto &payload_state = *gstate.payload_state;

	if (gstate.sorted_blocks.empty()) {
		return;
	}

	if (!state.initialized) {
		state.payload_chunk = gstate.sorted_blocks.back()->payload_chunk.get();
		// initialize parallel state (if any)
		state.parallel_state = nullptr;
		auto &task = context.task;
		// check if there is any parallel state to fetch
		state.parallel_state = nullptr;
		auto task_info = task.task_info.find(this);
		if (task_info != task.task_info.end()) {
			// parallel scan init
			state.parallel_state = task_info->second;
		}
		state.initialized = true;
	}

	if (!state.parallel_state) {
		// sequential scan
		auto next = MinValue((idx_t)STANDARD_VECTOR_SIZE, gstate.total_count - state.global_entry_idx);
		Scan(context.client, chunk, state, payload_state, next);
		if (chunk.size() != 0) {
			return;
		}
	} else {
		// parallel scan
		auto &parallel_state = *reinterpret_cast<OrderParallelState *>(state.parallel_state);
		do {
			idx_t next;
			{
				lock_guard<mutex> parallel_lock(parallel_state.lock);
				next = MinValue((idx_t)STANDARD_VECTOR_SIZE, gstate.total_count - parallel_state.global_entry_idx);
				// init local state using parallel state
				state.InitNextIndices();
				// update parallel state for the next scan
				UpdateStateIndices(next, *state.payload_chunk, parallel_state);
			}
			// now we scan using the local state
			Scan(context.client, chunk, state, payload_state, next);
			if (chunk.size() == 0) {
				break;
			} else {
				return;
			}
		} while (true);
	}
	// TODO: cleanup?
	D_ASSERT(chunk.size() == 0);
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
