#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"

#include <numeric>

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t estimated_cardinality)
    : PhysicalSink(PhysicalOperatorType::ORDER_BY, move(types), estimated_cardinality), orders(move(orders)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct SortingState {
	const idx_t num_cols;
	const idx_t entry_size;
	const idx_t comp_size;
	const bool all_constant;

	const vector<OrderType> order_types;
	const vector<OrderByNullType> order_by_null_types;
	const vector<LogicalType> types;
	const vector<BaseStatistics *> stats;

	const vector<bool> has_null;
	const vector<bool> constant_size;
	const vector<idx_t> col_size;
};

struct PayloadState {
	const bool all_constant;
	const idx_t validitymask_size;
	const idx_t entry_size;

	const idx_t rowdata_init_size;
};

class OrderGlobalState : public GlobalOperatorState {
public:
	explicit OrderGlobalState(BufferManager &buffer_manager)
	    : buffer_manager(buffer_manager), total_count(0), sorting_block_capacity(0),
	      payload_data_block_dims(std::make_pair(0, 0)), payload_offset_block_capacity(0) {
	}

	~OrderGlobalState() override;

	//! The lock for updating the order global state
	std::mutex lock;
	//! The buffer manager
	BufferManager &buffer_manager;

	//! Constants concerning sorting and/or payload data
	unique_ptr<SortingState> sorting_state;
	unique_ptr<PayloadState> payload_state;

	//! Sorted data
	vector<unique_ptr<SortedBlock>> sorted_blocks;
	vector<unique_ptr<SortedBlock>> sorted_blocks_temp;

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

	void Initialize(ClientContext &context, const SortingState &sorting_state, const PayloadState &payload_state) {
		auto &buffer_manager = BufferManager::GetBufferManager(context);
		// sorting block
		idx_t vectors_per_block =
		    (Storage::BLOCK_ALLOC_SIZE / sorting_state.entry_size + STANDARD_VECTOR_SIZE) / STANDARD_VECTOR_SIZE;
		sorting_block = make_unique<RowDataCollection>(buffer_manager, vectors_per_block * STANDARD_VECTOR_SIZE,
		                                               sorting_state.entry_size);
		// variable sorting column blocks
		for (idx_t i = 0; i < sorting_state.num_cols; i++) {
			if (sorting_state.constant_size[i]) {
				var_sorting_blocks.push_back(nullptr);
				var_sorting_sizes.push_back(nullptr);
			} else {
				var_sorting_blocks.push_back(
				    make_unique<RowDataCollection>(buffer_manager, Storage::BLOCK_ALLOC_SIZE / 8, 8));
				var_sorting_sizes.push_back(make_unique<RowDataCollection>(
				    buffer_manager, (idx_t)Storage::BLOCK_ALLOC_SIZE / sizeof(idx_t) + 1, sizeof(idx_t)));
			}
		}
		// payload block
		if (!payload_state.all_constant) {
			payload_block = make_unique<RowDataCollection>(buffer_manager, payload_state.rowdata_init_size / 8, 8);
			sizes_block = make_unique<RowDataCollection>(
			    buffer_manager, (idx_t)Storage::BLOCK_ALLOC_SIZE / sizeof(idx_t) + 1, sizeof(idx_t));
		} else {
			payload_block = make_unique<RowDataCollection>(
			    buffer_manager, payload_state.rowdata_init_size / payload_state.entry_size, payload_state.entry_size);
		}
		initialized = true;
	}

	bool Full(ClientContext &context, const SortingState &sorting_state, const PayloadState &payload_state) {
		// compute the size of the collected data (in bytes)
		idx_t size_in_bytes = sorting_block->count * sorting_state.entry_size;
		if (!sorting_state.all_constant) {
			for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
				if (!sorting_state.constant_size[col_idx]) {
					for (auto &block : var_sorting_blocks[col_idx]->blocks) {
						size_in_bytes += block.byte_offset;
					}
					size_in_bytes += var_sorting_sizes[col_idx]->count * sizeof(idx_t);
				}
			}
		}
		if (payload_state.all_constant) {
			size_in_bytes += payload_block->count * payload_state.entry_size;
		} else {
			for (auto &block : payload_block->blocks) {
				size_in_bytes += block.byte_offset;
			}
			size_in_bytes += sizes_block->count * sizeof(idx_t);
		}
		// get the max memory and number of threads
		auto &buffer_manager = BufferManager::GetBufferManager(context);
		auto &task_scheduler = TaskScheduler::GetScheduler(context);
		idx_t max_memory = buffer_manager.GetMaxMemory();
		idx_t num_threads = task_scheduler.NumberOfThreads();
		// memory usage per thread should scale with max mem / num threads
		// we take 30% of the max memory, to be conservative
		return size_in_bytes > (0.3 * max_memory / num_threads);
	}

	//! Sorting columns, and variable size sorting data (if any)
	unique_ptr<RowDataCollection> sorting_block = nullptr;
	vector<unique_ptr<RowDataCollection>> var_sorting_blocks;
	vector<unique_ptr<RowDataCollection>> var_sorting_sizes;

	//! Payload data (and payload entry sizes if there is variable size data)
	unique_ptr<RowDataCollection> payload_block = nullptr;
	unique_ptr<RowDataCollection> sizes_block = nullptr;

	//! Sorted data
	vector<unique_ptr<SortedBlock>> sorted_blocks;

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
	vector<LogicalType> order_l_types;
	vector<BaseStatistics *> stats;
	vector<bool> has_null;
	vector<bool> constant_size;
	vector<idx_t> col_sizes;
	for (auto &order : orders) {
		// global state ExpressionExecutor
		auto &expr = *order.expression;

		// sorting state
		order_types.push_back(order.type);
		order_by_null_types.push_back(order.null_order);
		order_l_types.push_back(expr.return_type);
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
				col_size = PhysicalOrder::STRING_RADIX_SIZE;
				break;
			default:
				throw NotImplementedException("Unable to order column with type %s", expr.return_type.ToString());
			}
		}
		has_null.push_back(true);

		// increment entry size with the column size
		if (has_null.back()) {
			col_size++;
		}
		entry_size += col_size;
		col_sizes.push_back(col_size);
	}
	// make room for an 'index' column at the end
	idx_t comp_size = entry_size;
	entry_size += sizeof(idx_t);
	bool all_constant = true;
	for (auto constant : constant_size) {
		all_constant = all_constant && constant;
	}
	state->sorting_state = unique_ptr<SortingState>(
	    new SortingState {orders.size(), entry_size, comp_size, all_constant, order_types, order_by_null_types,
	                      order_l_types, stats, has_null, constant_size, col_sizes});

	// init payload state
	entry_size = 0;
	idx_t validitymask_size = (types.size() + 7) / 8;
	entry_size += validitymask_size;
	all_constant = true;
	idx_t var_columns = 0;
	for (auto &type : types) {
		auto physical_type = type.InternalType();
		if (TypeIsConstantSize(physical_type)) {
			entry_size += GetTypeIdSize(physical_type);
		} else {
			all_constant = false;
			var_columns++;
		}
	}
	idx_t rowdata_init_size = Storage::BLOCK_ALLOC_SIZE;
	if (all_constant) {
		idx_t vectors_per_block =
		    (Storage::BLOCK_ALLOC_SIZE / entry_size + STANDARD_VECTOR_SIZE) / STANDARD_VECTOR_SIZE;
		rowdata_init_size = vectors_per_block * STANDARD_VECTOR_SIZE * entry_size;
	}
	state->payload_state =
	    unique_ptr<PayloadState>(new PayloadState {all_constant, validitymask_size, entry_size, rowdata_init_size});
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
		lstate.Initialize(context.client, *gstate.sorting_state, *gstate.payload_state);
	}

	// obtain sorting columns
	auto &sort = lstate.sort;
	lstate.executor.Execute(input, sort);

	// build and serialize sorting data
	lstate.sorting_block->Build(sort.size(), lstate.key_locations, nullptr);
	for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
		bool has_null = sorting_state.has_null[sort_col];
		bool nulls_first = sorting_state.order_by_null_types[sort_col] == OrderByNullType::NULLS_FIRST;
		bool desc = sorting_state.order_types[sort_col] == OrderType::DESCENDING;
		idx_t size_in_bytes = PhysicalOrder::STRING_RADIX_SIZE; // TODO: use actual string statistics
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
		RowDataCollection::ComputeEntrySizes(sort.data[sort_col], lstate.entry_sizes, sort.size());
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
	if (payload_state.all_constant) {
		lstate.payload_block->Build(input.size(), lstate.key_locations, nullptr);
	} else {
		RowDataCollection::ComputeEntrySizes(input, lstate.entry_sizes, payload_state.entry_size);
		lstate.sizes_block->Build(input.size(), lstate.key_locations, nullptr);
		for (idx_t i = 0; i < input.size(); i++) {
			Store<idx_t>(lstate.entry_sizes[i], lstate.key_locations[i]);
		}
		lstate.payload_block->Build(input.size(), lstate.key_locations, lstate.entry_sizes);
	}

	// serialize payload data
	for (idx_t i = 0; i < input.size(); i++) {
		memset(lstate.key_locations[i], -1, payload_state.validitymask_size);
		lstate.validitymask_locations[i] = lstate.key_locations[i];
		lstate.key_locations[i] += payload_state.validitymask_size;
	}
	for (idx_t payl_col = 0; payl_col < input.ColumnCount(); payl_col++) {
		lstate.payload_block->SerializeVector(input.data[payl_col], input.size(), *lstate.sel_ptr, input.size(),
		                                      payl_col, lstate.key_locations, lstate.validitymask_locations);
	}

	// when sorting data reaches a certain size, we sort it
	if (lstate.Full(context.client, sorting_state, payload_state)) {
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

struct SortedData {
public:
	SortedData(BufferManager &buffer_manager, bool constant_size, idx_t entry_size)
	    : buffer_manager(buffer_manager), constant_size(constant_size), entry_size(entry_size), block_idx(0),
	      entry_idx(0) {
	}

	idx_t Count() {
		idx_t count = std::accumulate(data_blocks.begin(), data_blocks.end(), 0,
		                              [](idx_t a, const RowDataBlock &b) { return a + b.count; });
#ifdef DEBUG
		if (!constant_size) {
			D_ASSERT(count == std::accumulate(offset_blocks.begin(), offset_blocks.end(), (idx_t)0,
			                                  [](idx_t a, const RowDataBlock &b) { return a + b.count; }));
		}
#endif
		return count;
	}

	//! Initialize this to write data to during the merge
	void InitializeWrite(const std::pair<idx_t, idx_t> dims_p, const idx_t offset_capacity_p) {
		data_dims = dims_p;
		offset_capacity = offset_capacity_p;
		CreateBlock();
	}

	//! Initialize new block to write to
	void CreateBlock() {
		data_blocks.emplace_back(buffer_manager, data_dims.first, data_dims.second);
		if (!constant_size) {
			// offset blocks need 1 more capacity to store the size of the final entry
			offset_blocks.emplace_back(buffer_manager, offset_capacity, sizeof(idx_t), 1);
		}
	}

	data_ptr_t DataPtr() {
		return dataptr + offsets[entry_idx];
	}

	void Pin() {
		D_ASSERT(block_idx < data_blocks.size());
		data_handle = buffer_manager.Pin(data_blocks[block_idx].block);
		offset_handle = buffer_manager.Pin(offset_blocks[block_idx].block);
		dataptr = data_handle->Ptr();
		offsets = (idx_t *)offset_handle->Ptr();
	}

	void Advance() {
		entry_idx++;
		if (entry_idx == data_blocks[block_idx].count) {
			block_idx++;
			entry_idx = 0;
			if (block_idx < data_blocks.size()) {
				Pin();
			} else {
				UnpinAndReset(block_idx, entry_idx);
			}
		}
	}

	void UnpinAndReset(idx_t block_idx_to, idx_t entry_idx_to) {
		data_handle = nullptr;
		offset_handle = nullptr;
		dataptr = nullptr;
		offsets = nullptr;
		block_idx = block_idx_to;
		entry_idx = entry_idx_to;
	}

private:
	//! Buffer manager and constants
	BufferManager &buffer_manager;

	std::pair<idx_t, idx_t> data_dims;
	idx_t offset_capacity;

	unique_ptr<BufferHandle> data_handle = nullptr;
	unique_ptr<BufferHandle> offset_handle = nullptr;

	data_ptr_t dataptr;
	idx_t *offsets;

public:
	const bool constant_size;
	const idx_t entry_size;

	//! Data and offset blocks
	vector<RowDataBlock> data_blocks;
	vector<RowDataBlock> offset_blocks;

	//! Data
	idx_t block_idx;
	idx_t entry_idx;
};

struct SortedBlock {
public:
	SortedBlock(BufferManager &buffer_manager, const SortingState &sorting_state, const PayloadState &payload_state)
	    : block_idx(0), entry_idx(0), buffer_manager(buffer_manager), sorting_state(sorting_state),
	      payload_state(payload_state) {
		if (!sorting_state.all_constant) {
			for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
				if (!sorting_state.constant_size[col_idx]) {
					var_sorting_cols.push_back(make_unique<SortedData>(buffer_manager, false, 0));
				} else {
					var_sorting_cols.push_back(nullptr);
				}
			}
		}
		payload_data = make_unique<SortedData>(buffer_manager, payload_state.all_constant, payload_state.entry_size);
	}

	idx_t Count() {
		idx_t count = std::accumulate(sorting_blocks.begin(), sorting_blocks.end(), 0,
		                              [](idx_t a, const RowDataBlock &b) { return a + b.count; });
#ifdef DEBUG
		vector<idx_t> counts;
		counts.push_back(count);
		if (!sorting_state.all_constant) {
			for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
				if (!sorting_state.constant_size[col_idx]) {
					counts.push_back(var_sorting_cols[col_idx]->Count());
				}
			}
		}
		counts.push_back(payload_data->Count());
		D_ASSERT(std::equal(counts.begin() + 1, counts.end(), counts.begin()));
#endif
		return count;
	}

	idx_t Remaining() {
		idx_t remaining = 0;
		if (block_idx >= sorting_blocks.size()) {
			return remaining;
		}
		remaining += sorting_blocks[block_idx].count - entry_idx;
		for (idx_t i = block_idx + 1; i < sorting_blocks.size(); i++) {
			remaining += sorting_blocks[i].count;
		}
		return remaining;
	}

	//! Initialize this block to write data to
	void InitializeWrite(const OrderGlobalState &state) {
		capacity = state.sorting_block_capacity;
		CreateBlock();
		if (!sorting_state.all_constant) {
			for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
				if (!sorting_state.constant_size[col_idx]) {
					var_sorting_cols[col_idx]->InitializeWrite(state.var_sorting_data_block_dims[col_idx],
					                                           state.var_sorting_offset_block_capacity[col_idx]);
				}
			}
		}
		payload_data->InitializeWrite(state.payload_data_block_dims, state.payload_offset_block_capacity);
	}

	//! Init new block to write to
	void CreateBlock() {
		sorting_blocks.emplace_back(buffer_manager, capacity, sorting_state.entry_size);
	}

	//! Cleanup sorting data
	void UnregisterSortingBlocks() {
		for (auto &block : sorting_blocks) {
			buffer_manager.UnregisterBlock(block.block->BlockId(), true);
		}
		if (!sorting_state.all_constant) {
			for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
				if (!sorting_state.constant_size[col_idx]) {
					for (auto &block : var_sorting_cols[col_idx]->data_blocks) {
						buffer_manager.UnregisterBlock(block.block->BlockId(), true);
					}
					for (auto &block : var_sorting_cols[col_idx]->offset_blocks) {
						buffer_manager.UnregisterBlock(block.block->BlockId(), true);
					}
				}
			}
		}
	}

	//! Cleanup payload data
	void UnregisterPayloadBlocks() {
		for (auto &block : payload_data->data_blocks) {
			buffer_manager.UnregisterBlock(block.block->BlockId(), true);
		}
		if (!payload_state.all_constant) {
			for (auto &block : payload_data->offset_blocks) {
				buffer_manager.UnregisterBlock(block.block->BlockId(), true);
			}
		}
	}

public:
	//! Memcmp-able representation of sorting columns
	vector<RowDataBlock> sorting_blocks;
	idx_t block_idx;
	idx_t entry_idx;

	//! Variable size sorting columns
	vector<unique_ptr<SortedData>> var_sorting_cols;

	//! Payload columns and their offsets
	unique_ptr<SortedData> payload_data;

private:
	//! Buffer manager, and sorting state constants
	BufferManager &buffer_manager;
	const SortingState &sorting_state;
	const PayloadState &payload_state;

	//! Handle and ptr for sorting_blocks
	unique_ptr<BufferHandle> sorting_handle;

	idx_t capacity;
};

OrderGlobalState::~OrderGlobalState() {
	std::lock_guard<mutex> glock(lock);
	for (auto &sb : sorted_blocks) {
		sb->UnregisterPayloadBlocks();
	}
}

static void ComputeCountAndCapacity(RowDataCollection &row_data, bool constant_size, idx_t &count, idx_t &capacity) {
	const idx_t &entry_size = row_data.entry_size;
	count = 0;
	idx_t total_size = 0;
	for (const auto &block : row_data.blocks) {
		count += block.count;
		if (constant_size) {
			total_size += block.count * entry_size;
		} else {
			total_size += block.byte_offset;
		}
	}

	if (constant_size) {
		capacity = MaxValue(Storage::BLOCK_ALLOC_SIZE / entry_size + 1, count);
	} else {
		capacity = MaxValue(Storage::BLOCK_ALLOC_SIZE / entry_size, total_size / entry_size + 1);
	}
}

static RowDataBlock ConcatenateBlocks(BufferManager &buffer_manager, RowDataCollection &row_data, bool constant_size) {
	idx_t total_count;
	idx_t capacity;
	ComputeCountAndCapacity(row_data, constant_size, total_count, capacity);
	const idx_t &entry_size = row_data.entry_size;

	RowDataBlock new_block(buffer_manager, capacity, entry_size);
	new_block.count = total_count;
	auto new_block_handle = buffer_manager.Pin(new_block.block);
	data_ptr_t new_block_ptr = new_block_handle->Ptr();

	for (auto &block : row_data.blocks) {
		auto block_handle = buffer_manager.Pin(block.block);
		if (constant_size) {
			memcpy(new_block_ptr, block_handle->Ptr(), block.count * entry_size);
			new_block_ptr += block.count * entry_size;
		} else {
			memcpy(new_block_ptr, block_handle->Ptr(), block.byte_offset);
			new_block_ptr += block.byte_offset;
		}
		buffer_manager.UnregisterBlock(block.block->BlockId(), true);
	}
	row_data.blocks.clear();
	row_data.count = 0;
	return new_block;
}

static RowDataBlock SizesToOffsets(BufferManager &buffer_manager, RowDataCollection &row_data) {
	idx_t total_count;
	idx_t capacity;
	ComputeCountAndCapacity(row_data, true, total_count, capacity);
	// offsets block must store the size of the final entry, therefore need 1 more capacity
	capacity++;

	const idx_t &entry_size = row_data.entry_size;
	RowDataBlock new_block(buffer_manager, capacity, entry_size);
	new_block.count = total_count;
	auto new_block_handle = buffer_manager.Pin(new_block.block);
	data_ptr_t new_block_ptr = new_block_handle->Ptr();
	for (auto &block : row_data.blocks) {
		auto block_handle = buffer_manager.Pin(block.block);
		memcpy(new_block_ptr, block_handle->Ptr(), block.count * entry_size);
		new_block_ptr += block.count * entry_size;
		buffer_manager.UnregisterBlock(block.block->BlockId(), true);
	}
	row_data.blocks.clear();
	row_data.count = 0;
	// convert sizes to offsets
	idx_t *offsets = (idx_t *)new_block_handle->Ptr();
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

static int CompareStrings(data_ptr_t &left_ptr, data_ptr_t &right_ptr, const int &order) {
	// read string lengths
	uint32_t left_size = Load<uint32_t>(left_ptr);
	uint32_t right_size = Load<uint32_t>(right_ptr);
	left_ptr += string_t::PREFIX_LENGTH;
	right_ptr += string_t::PREFIX_LENGTH;
	// construct strings
	string_t left_val((const char *)left_ptr, left_size);
	string_t right_val((const char *)right_ptr, right_size);
	// do the comparison
	int comp_res;
	if (Equals::Operation<string_t>(left_val, right_val)) {
		comp_res = 0;
	} else if (LessThan::Operation<string_t>(left_val, right_val)) {
		comp_res = -1;
	} else {
		comp_res = 1;
	}
	return order * comp_res;
}

static void BreakStringTies(BufferManager &buffer_manager, const data_ptr_t dataptr, const idx_t &start,
                            const idx_t &end, const idx_t &tie_col, bool ties[], const data_ptr_t var_dataptr,
                            const data_ptr_t offsets_ptr, const SortingState &sorting_state) {
	idx_t tie_col_offset = 0;
	for (idx_t i = 0; i < tie_col; i++) {
		tie_col_offset += sorting_state.col_size[i];
	}
	// if the strings are tied because they are NULL we don't need to break to tie
	if (sorting_state.has_null[tie_col]) {
		char *validity = (char *)dataptr + start * sorting_state.entry_size + tie_col_offset;
		if (sorting_state.order_by_null_types[tie_col] == OrderByNullType::NULLS_FIRST && *validity == 0) {
			// NULLS_FIRST, therefore null is encoded as 0 - we can't break null ties
			return;
		} else if (sorting_state.order_by_null_types[tie_col] == OrderByNullType::NULLS_LAST && *validity == 1) {
			// NULLS_LAST, therefore null is encoded as 1 - we can't break null ties
			return;
		}
		tie_col_offset++;
	}

	// if the tied strings are smaller than the prefix size, we don't need to break the ties
	char *prefix_chars = (char *)dataptr + start * sorting_state.entry_size + tie_col_offset;
	const char null_char = sorting_state.order_types[tie_col] == OrderType::ASCENDING ? 0 : -1;
	for (idx_t i = 0; i < PhysicalOrder::STRING_RADIX_SIZE; i++) {
		if (prefix_chars[i] == null_char) {
			return;
		}
	}

	// fill pointer array for sorting
	auto ptr_block =
	    buffer_manager.Allocate(MaxValue((end - start) * sizeof(data_ptr_t), (idx_t)Storage::BLOCK_ALLOC_SIZE));
	auto entry_ptrs = (data_ptr_t *)ptr_block->Ptr();
	for (idx_t i = start; i < end; i++) {
		entry_ptrs[i - start] = dataptr + i * sorting_state.entry_size;
	}
	// slow pointer-based sorting
	const int order = sorting_state.order_types[tie_col] == OrderType::DESCENDING ? -1 : 1;
	const idx_t sorting_size = sorting_state.comp_size;
	const idx_t *offsets = (idx_t *)offsets_ptr;
	std::sort(entry_ptrs, entry_ptrs + end - start,
	          [&var_dataptr, &offsets, &order, &sorting_size](const data_ptr_t l, const data_ptr_t r) {
		          // use indices to find strings in blob
		          idx_t left_idx = Load<idx_t>(l + sorting_size);
		          idx_t right_idx = Load<idx_t>(r + sorting_size);
		          data_ptr_t left_ptr = var_dataptr + offsets[left_idx];
		          data_ptr_t right_ptr = var_dataptr + offsets[right_idx];
		          return CompareStrings(left_ptr, right_ptr, order) < 0;
	          });
	// re-order
	auto temp_block =
	    buffer_manager.Allocate(MaxValue((end - start) * sorting_state.entry_size, (idx_t)Storage::BLOCK_ALLOC_SIZE));
	data_ptr_t temp_ptr = temp_block->Ptr();
	for (idx_t i = 0; i < end - start; i++) {
		memcpy(temp_ptr, entry_ptrs[i], sorting_state.entry_size);
		temp_ptr += sorting_state.entry_size;
	}
	memcpy(dataptr + start * sorting_state.entry_size, temp_block->Ptr(), (end - start) * sorting_state.entry_size);

	// determine if there are still ties (if this is not the last column)
	if (tie_col < sorting_state.num_cols - 1) {
		data_ptr_t idx_ptr = dataptr + start * sorting_state.entry_size + sorting_size;

		idx_t current_idx = Load<idx_t>(idx_ptr);
		data_ptr_t current_ptr = var_dataptr + offsets[current_idx];
		uint32_t current_size = Load<uint32_t>(current_ptr);
		current_ptr += string_t::PREFIX_LENGTH;
		string_t current_val((const char *)current_ptr, current_size);
		for (idx_t i = 0; i < end - start - 1; i++) {
			idx_ptr += sorting_state.entry_size;

			// load next entry
			idx_t next_idx = Load<idx_t>(idx_ptr);
			data_ptr_t next_ptr = var_dataptr + offsets[next_idx];
			uint32_t next_size = Load<uint32_t>(next_ptr);
			next_ptr += string_t::PREFIX_LENGTH;
			string_t next_val((const char *)next_ptr, next_size);

			ties[start + i] = Equals::Operation<string_t>(current_val, next_val);
			current_size = next_size;
			current_val = next_val;
		}
	}
}

static void BreakTies(BufferManager &buffer_manager, SortedBlock &cb, bool ties[], data_ptr_t dataptr,
                      const idx_t &count, const idx_t &tie_col, const SortingState &sorting_state) {
	D_ASSERT(!ties[count - 1]);
	auto &var_data_block = cb.var_sorting_cols[tie_col]->data_blocks.back();
	auto &var_offsets_block = cb.var_sorting_cols[tie_col]->offset_blocks.back();
	auto var_block_handle = buffer_manager.Pin(var_data_block.block);
	auto var_sizes_handle = buffer_manager.Pin(var_offsets_block.block);
	const data_ptr_t var_dataptr = var_block_handle->Ptr();
	const data_ptr_t offsets_ptr = var_sizes_handle->Ptr();

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
		switch (sorting_state.types[tie_col].InternalType()) {
		case PhysicalType::VARCHAR:
			BreakStringTies(buffer_manager, dataptr, i, j + 1, tie_col, ties, var_dataptr, offsets_ptr, sorting_state);
			break;
		default:
			throw NotImplementedException("Cannot sort variable size column with type %s",
			                              sorting_state.types[tie_col].ToString());
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
	D_ASSERT(col_offset + tie_size <= sorting_state.comp_size);
	// align dataptr
	dataptr += col_offset;
	idx_t i = 0;
	for (; i + 7 < count - 1; i += 8) {
		// fixed size inner loop to allow unrolling
		for (idx_t j = 0; j < 8; j++) {
			ties[i + j] = ties[i + j] && memcmp(dataptr, dataptr + sorting_state.entry_size, tie_size) == 0;
			dataptr += sorting_state.entry_size;
		}
	}
	for (; i < count - 1; i++) {
		ties[i] = ties[i] && memcmp(dataptr, dataptr + sorting_state.entry_size, tie_size) == 0;
		dataptr += sorting_state.entry_size;
	}
	ties[count - 1] = false;
}

//! Textbook LSD radix sort
static void RadixSort(BufferManager &buffer_manager, data_ptr_t dataptr, const idx_t &count, const idx_t &col_offset,
                      const idx_t &sorting_size, const SortingState &sorting_state) {
	auto temp_block =
	    buffer_manager.Allocate(MaxValue(count * sorting_state.entry_size, (idx_t)Storage::BLOCK_ALLOC_SIZE));
	data_ptr_t temp = temp_block->Ptr();
	bool swap = false;

	idx_t counts[256];
	uint8_t byte;
	for (idx_t offset = col_offset + sorting_size - 1; offset + 1 > col_offset; offset--) {
		// init to 0
		memset(counts, 0, sizeof(counts));
		// collect counts
		for (idx_t i = 0; i < count; i++) {
			byte = *(dataptr + i * sorting_state.entry_size + offset);
			counts[byte]++;
		}
		// compute offsets from counts
		for (idx_t val = 1; val < 256; val++) {
			counts[val] = counts[val] + counts[val - 1];
		}
		// re-order the data in temporary array
		for (idx_t i = count; i > 0; i--) {
			byte = *(dataptr + (i - 1) * sorting_state.entry_size + offset);
			memcpy(temp + (counts[byte] - 1) * sorting_state.entry_size, dataptr + (i - 1) * sorting_state.entry_size,
			       sorting_state.entry_size);
			counts[byte]--;
		}
		std::swap(dataptr, temp);
		swap = !swap;
	}
	// move data back to original buffer (if it was swapped)
	if (swap) {
		memcpy(temp, dataptr, count * sorting_state.entry_size);
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
		RadixSort(buffer_manager, dataptr + i * sorting_state.entry_size, j - i + 1, col_offset, sorting_size,
		          sorting_state);
		i = j;
	}
}

static void SortInMemory(BufferManager &buffer_manager, SortedBlock &cb, const SortingState &sorting_state) {
	auto &block = cb.sorting_blocks.back();
	const auto &count = block.count;
	auto handle = buffer_manager.Pin(block.block);
	const auto dataptr = handle->Ptr();

	// assign an index to each row
	data_ptr_t idx_dataptr = dataptr + sorting_state.comp_size;
	for (idx_t i = 0; i < count; i++) {
		Store<idx_t>(i, idx_dataptr);
		idx_dataptr += sorting_state.entry_size;
	}

	if (sorting_state.all_constant) {
		RadixSort(buffer_manager, dataptr, count, 0, sorting_state.comp_size, sorting_state);
		return;
	}

	idx_t sorting_size = 0;
	idx_t col_offset = 0;
	unique_ptr<BufferHandle> ties_handle = nullptr;
	bool *ties = nullptr;
	const idx_t num_cols = sorting_state.num_cols;
	for (idx_t i = 0; i < num_cols; i++) {
		sorting_size += sorting_state.col_size[i];
		if (sorting_state.constant_size[i] && i < num_cols - 1) {
			// add columns to the sort until we reach a variable size column, or the last column
			continue;
		}

		if (!ties) {
			// this is the first sort
			RadixSort(buffer_manager, dataptr, count, col_offset, sorting_size, sorting_state);
			ties_handle = buffer_manager.Allocate(MaxValue(count, (idx_t)Storage::BLOCK_ALLOC_SIZE));
			ties = (bool *)ties_handle->Ptr();
			std::fill_n(ties, count - 1, true);
			ties[count - 1] = false;
		} else {
			// for subsequent sorts, we only have to subsort the tied tuples
			SubSortTiedTuples(buffer_manager, dataptr, count, col_offset, sorting_size, ties, sorting_state);
		}

		if (sorting_state.constant_size[i] && i == num_cols - 1) {
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

static void ReOrder(BufferManager &buffer_manager, SortedData &sd, data_ptr_t sorting_ptr,
                    const SortingState &sorting_state) {
	const idx_t &count = sd.data_blocks.back().count;

	auto &unordered_data_block = sd.data_blocks.back();
	auto unordered_data_handle = buffer_manager.Pin(unordered_data_block.block);
	const data_ptr_t unordered_data_ptr = unordered_data_handle->Ptr();

	RowDataBlock reordered_data_block(buffer_manager, unordered_data_block.capacity, unordered_data_block.entry_size);
	reordered_data_block.count = count;
	auto ordered_data_handle = buffer_manager.Pin(reordered_data_block.block);
	data_ptr_t ordered_data_ptr = ordered_data_handle->Ptr();

	if (sd.constant_size) {
		const idx_t entry_size = unordered_data_block.entry_size;
		for (idx_t i = 0; i < count; i++) {
			idx_t index = Load<idx_t>(sorting_ptr);
			memcpy(ordered_data_ptr, unordered_data_ptr + index * entry_size, entry_size);
			ordered_data_ptr += entry_size;
			sorting_ptr += sorting_state.entry_size;
		}
	} else {
		// variable size data: we need offsets too
		reordered_data_block.byte_offset = unordered_data_block.byte_offset;
		auto &unordered_offset_block = sd.offset_blocks.back();
		auto unordered_offset_handle = buffer_manager.Pin(unordered_offset_block.block);
		idx_t *unordered_offsets = (idx_t *)unordered_offset_handle->Ptr();

		RowDataBlock reordered_offset_block(buffer_manager, unordered_offset_block.capacity,
		                                    unordered_offset_block.entry_size);
		reordered_offset_block.count = count;
		auto reordered_offset_handle = buffer_manager.Pin(reordered_offset_block.block);
		idx_t *reordered_offsets = (idx_t *)reordered_offset_handle->Ptr();
		reordered_offsets[0] = 0;

		for (idx_t i = 0; i < count; i++) {
			idx_t index = Load<idx_t>(sorting_ptr);
			idx_t size = unordered_offsets[index + 1] - unordered_offsets[index];
			memcpy(ordered_data_ptr, unordered_data_ptr + unordered_offsets[index], size);
			ordered_data_ptr += size;
			reordered_offsets[i + 1] = reordered_offsets[i] + size;
			sorting_ptr += sorting_state.entry_size;
		}
		// replace offset block
		buffer_manager.UnregisterBlock(unordered_offset_block.block->BlockId(), true);
		sd.offset_blocks.clear();
		sd.offset_blocks.push_back(move(reordered_offset_block));
	}
	// replace data block
	buffer_manager.UnregisterBlock(unordered_data_block.block->BlockId(), true);
	sd.data_blocks.clear();
	sd.data_blocks.push_back(move(reordered_data_block));
}

//! Use the ordered sorting data to re-order the rest of the data
static void ReOrder(ClientContext &context, SortedBlock &sb, const SortingState &sorting_state,
                    const PayloadState &payload_state) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto sorting_handle = buffer_manager.Pin(sb.sorting_blocks.back().block);
	const data_ptr_t sorting_ptr = sorting_handle->Ptr() + sorting_state.comp_size;

	// re-order variable size sorting columns
	for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
		if (!sorting_state.constant_size[col_idx]) {
			ReOrder(buffer_manager, *sb.var_sorting_cols[col_idx], sorting_ptr, sorting_state);
		}
	}
	// and the payload
	ReOrder(buffer_manager, *sb.payload_data, sorting_ptr, sorting_state);
}

void PhysicalOrder::SortLocalState(ClientContext &context, OrderLocalState &lstate, const SortingState &sorting_state,
                                   const PayloadState &payload_state) {
	const idx_t &count = lstate.sorting_block->count;
	D_ASSERT(count == lstate.payload_block->count);
	if (lstate.sorting_block->count == 0) {
		return;
	}

	// copy all data to SortedBlocks
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto sb = make_unique<SortedBlock>(buffer_manager, sorting_state, payload_state);
	// fixed-size sorting data
	auto sorting_block = ConcatenateBlocks(buffer_manager, *lstate.sorting_block, true);
	sb->sorting_blocks.push_back(move(sorting_block));
	// variable size sorting columns
	for (idx_t i = 0; i < lstate.var_sorting_blocks.size(); i++) {
		if (!sorting_state.constant_size[i]) {
			auto &sd = sb->var_sorting_cols[i];
			auto &row_data = *lstate.var_sorting_blocks[i];
			auto new_block = ConcatenateBlocks(buffer_manager, row_data, false);
			auto &sizes_data = *lstate.var_sorting_sizes[i];
			auto offsets_block = SizesToOffsets(buffer_manager, sizes_data);
			sd->data_blocks.push_back(move(new_block));
			sd->offset_blocks.push_back(move(offsets_block));
		}
	}
	// payload data
	auto payload_block = ConcatenateBlocks(buffer_manager, *lstate.payload_block, payload_state.all_constant);
	sb->payload_data->data_blocks.push_back(move(payload_block));
	if (!payload_state.all_constant) {
		auto offsets_block = SizesToOffsets(buffer_manager, *lstate.sizes_block);
		sb->payload_data->offset_blocks.push_back(move(offsets_block));
	}

	// now perform the actual sort
	SortInMemory(buffer_manager, *sb, sorting_state);

	// re-order before the merge sort
	ReOrder(context, *sb, sorting_state, payload_state);

	// add the sorted block to the global state
	lstate.sorted_blocks.push_back(move(sb));
}

int CompareVarCol(const idx_t &col_idx, const data_ptr_t &l_ptr, const data_ptr_t &r_ptr, data_ptr_t l_var_ptr,
                  data_ptr_t r_var_ptr, const SortingState &sorting_state) {
	if (sorting_state.has_null[col_idx]) {
		// check if the var col is tie because they are NULL
		const char validity = *l_ptr;
		if (sorting_state.order_by_null_types[col_idx] == OrderByNullType::NULLS_FIRST && validity == 0) {
			// NULLS_FIRST, therefore null is encoded as 0 - we can't break null ties
			return 0;
		} else if (sorting_state.order_by_null_types[col_idx] == OrderByNullType::NULLS_LAST && validity == 1) {
			// NULLS_LAST, therefore null is encoded as 1 - we can't break null ties
			return 0;
		}
	}
	// now do the comparison
	const int order = sorting_state.order_types[col_idx] == OrderType::DESCENDING ? -1 : 1;
	switch (sorting_state.types[col_idx].InternalType()) {
	case PhysicalType::VARCHAR: {
		return CompareStrings(l_var_ptr, r_var_ptr, order);
	}
	default:
		throw NotImplementedException("Unimplemented compare for type %s", sorting_state.types[col_idx].ToString());
	}
}

class PhysicalOrderMergeTask : public Task {
public:
	PhysicalOrderMergeTask(Pipeline &parent_p, ClientContext &context_p, OrderGlobalState &state_p, SortedBlock &left_p,
	                       SortedBlock &right_p)
	    : parent(parent_p), context(context_p), buffer_manager(BufferManager::GetBufferManager(context_p)),
	      state(state_p), sorting_state(*state_p.sorting_state), payload_state(*state_p.payload_state), left(left_p),
	      right(right_p) {
		result = make_unique<SortedBlock>(buffer_manager, sorting_state, payload_state);
	}

	void Execute() override {
		result->InitializeWrite(state);
		bool left_smaller[PhysicalOrder::MERGE_STRIDE];
		idx_t next_entry_sizes[PhysicalOrder::MERGE_STRIDE];
		while (true) {
			auto l_remaining = left.Remaining();
			auto r_remaining = right.Remaining();
			if (l_remaining + r_remaining == 0) {
				break;
			}
			const idx_t next = MinValue(l_remaining + r_remaining, PhysicalOrder::MERGE_STRIDE);
			if (l_remaining != 0 && r_remaining != 0) {
				// no need to compute the merge if either left or right is exhausted
				ComputeMerge(next, left_smaller);
			}
			Merge(next, left_smaller);
			if (!sorting_state.all_constant) {
				for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
					if (!sorting_state.constant_size[col_idx]) {
						Merge(*result->var_sorting_cols[col_idx], *left.var_sorting_cols[col_idx],
						      *right.var_sorting_cols[col_idx], next, left_smaller, next_entry_sizes);
					}
				}
			}
			Merge(*result->payload_data, *left.payload_data, *right.payload_data, next, left_smaller, next_entry_sizes);
		}
		D_ASSERT(result->Count() == left.Count() + right.Count());

		lock_guard<mutex> glock(state.lock);
		state.sorted_blocks_temp.push_back(move(result));
		parent.finished_tasks++;
		if (parent.total_tasks == parent.finished_tasks) {
			for (auto &sb : state.sorted_blocks) {
				sb->UnregisterSortingBlocks();
				sb->UnregisterPayloadBlocks();
			}
			state.sorted_blocks.clear();
			for (auto &block : state.sorted_blocks_temp) {
				state.sorted_blocks.push_back(move(block));
			}
			state.sorted_blocks_temp.clear();
			PhysicalOrder::ScheduleMergeTasks(parent, context, state);
		}
	}

	//! Computes how the next 'count' tuples should be merged
	void ComputeMerge(const idx_t &count, bool *left_smaller) {
		idx_t l_block_idx = left.block_idx;
		idx_t r_block_idx = right.block_idx;
		idx_t l_entry_idx = left.entry_idx;
		idx_t r_entry_idx = right.entry_idx;

		// these are always used
		RowDataBlock *l_block = nullptr;
		RowDataBlock *r_block = nullptr;
		unique_ptr<BufferHandle> l_block_handle;
		unique_ptr<BufferHandle> r_block_handle;
		data_ptr_t l_ptr;
		data_ptr_t r_ptr;

		// these are only used for variable size sorting data
		vector<idx_t> l_var_block_idxs(sorting_state.num_cols);
		vector<idx_t> r_var_block_idxs(sorting_state.num_cols);
		vector<idx_t> l_var_entry_idxs(sorting_state.num_cols);
		vector<idx_t> r_var_entry_idxs(sorting_state.num_cols);
		for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
			if (!sorting_state.constant_size[col_idx]) {
				// save indices to restore after computing the merge
				l_var_block_idxs[col_idx] = left.var_sorting_cols[col_idx]->block_idx;
				r_var_block_idxs[col_idx] = right.var_sorting_cols[col_idx]->block_idx;
				l_var_entry_idxs[col_idx] = left.var_sorting_cols[col_idx]->entry_idx;
				r_var_entry_idxs[col_idx] = right.var_sorting_cols[col_idx]->entry_idx;
			}
		}

		idx_t compared = 0;
		while (compared < count) {
			if (l_block_idx == left.sorting_blocks.size() || r_block_idx == right.sorting_blocks.size()) {
				// one of the sides is exhausted, no need to compare
				break;
			}
			// pin the fixed-size sorting data
			if (l_block_idx < left.sorting_blocks.size()) {
				l_block = &left.sorting_blocks[l_block_idx];
				l_block_handle = buffer_manager.Pin(l_block->block);
				l_ptr = l_block_handle->Ptr() + l_entry_idx * sorting_state.entry_size;
			}
			if (r_block_idx < right.sorting_blocks.size()) {
				r_block = &right.sorting_blocks[r_block_idx];
				r_block_handle = buffer_manager.Pin(r_block->block);
				r_ptr = r_block_handle->Ptr() + r_entry_idx * sorting_state.entry_size;
			}
			const idx_t &l_count = l_block ? l_block->count : 0;
			const idx_t &r_count = r_block ? r_block->count : 0;
			// compute the merge
			if (sorting_state.all_constant) {
				// all sorting columns are constant size
				if (l_block_idx < left.sorting_blocks.size() && r_block_idx < right.sorting_blocks.size()) {
					// neither left or right side is exhausted - compare in loop
					for (; compared < count && l_entry_idx < l_count && r_entry_idx < r_count; compared++) {
						left_smaller[compared] = memcmp(l_ptr, r_ptr, sorting_state.comp_size) < 0;
						const bool &l_smaller = left_smaller[compared];
						const bool r_smaller = !l_smaller;
						left_smaller[compared] = l_smaller;
						// use comparison bool (0 or 1) to increment entries and pointers
						l_entry_idx += l_smaller;
						r_entry_idx += r_smaller;
						l_ptr += l_smaller * sorting_state.entry_size;
						r_ptr += r_smaller * sorting_state.entry_size;
					}
				}
			} else {
				// there are variable sized sorting columns, pin the var blocks
				if (l_block_idx < left.sorting_blocks.size()) {
					for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
						if (!sorting_state.constant_size[col_idx]) {
							left.var_sorting_cols[col_idx]->Pin();
						}
					}
				}
				if (r_block_idx < right.sorting_blocks.size()) {
					for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
						if (!sorting_state.constant_size[col_idx]) {
							right.var_sorting_cols[col_idx]->Pin();
						}
					}
				}
				if (l_block_idx < left.sorting_blocks.size() && r_block_idx < right.sorting_blocks.size()) {
					for (; compared < count && l_entry_idx < l_count && r_entry_idx < r_count; compared++) {
						// compare the sorting columns one by one
						int comp_res;
						data_ptr_t l_ptr_offset = l_ptr;
						data_ptr_t r_ptr_offset = r_ptr;
						for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
							comp_res = memcmp(l_ptr_offset, r_ptr_offset, sorting_state.col_size[col_idx]);
							if (comp_res == 0 && !sorting_state.constant_size[col_idx]) {
								comp_res = CompareVarCol(col_idx, l_ptr_offset, r_ptr_offset,
								                         left.var_sorting_cols[col_idx]->DataPtr(),
								                         right.var_sorting_cols[col_idx]->DataPtr(), sorting_state);
							}
							if (comp_res != 0) {
								break;
							}
							l_ptr_offset += sorting_state.col_size[col_idx];
							r_ptr_offset += sorting_state.col_size[col_idx];
						}
						// update for next iteration using the comparison bool
						left_smaller[compared] = comp_res < 0;
						const bool &l_smaller = left_smaller[compared];
						const bool r_smaller = !l_smaller;
						l_entry_idx += l_smaller;
						r_entry_idx += r_smaller;
						l_ptr += l_smaller * sorting_state.entry_size;
						r_ptr += r_smaller * sorting_state.entry_size;
						// and the var sorting columns
						if (l_smaller) {
							for (idx_t col_idx = 0; col_idx < sorting_state.constant_size.size(); col_idx++) {
								if (!sorting_state.constant_size[col_idx]) {
									left.var_sorting_cols[col_idx]->Advance();
								}
							}
						} else {
							for (idx_t col_idx = 0; col_idx < sorting_state.constant_size.size(); col_idx++) {
								if (!sorting_state.constant_size[col_idx]) {
									right.var_sorting_cols[col_idx]->Advance();
								}
							}
						}
					}
				}
			}
			// move to the next block (if needed)
			if (l_block_idx < left.sorting_blocks.size() && l_entry_idx == l_count) {
				l_block_idx++;
				l_entry_idx = 0;
			}
			if (r_block_idx < right.sorting_blocks.size() && r_entry_idx == r_count) {
				r_block_idx++;
				r_entry_idx = 0;
			}
		}
		// reset block indices before the actual merge
		if (!sorting_state.all_constant) {
			for (idx_t col_idx = 0; col_idx < sorting_state.num_cols; col_idx++) {
				if (!sorting_state.constant_size[col_idx]) {
					left.var_sorting_cols[col_idx]->UnpinAndReset(l_var_block_idxs[col_idx], l_var_entry_idxs[col_idx]);
					right.var_sorting_cols[col_idx]->UnpinAndReset(r_var_block_idxs[col_idx],
					                                               r_var_entry_idxs[col_idx]);
				}
			}
		}
	}

	//! Merges the fixed size sorting blocks
	void Merge(const idx_t &count, const bool *left_smaller) {
		RowDataBlock *l_block = nullptr;
		RowDataBlock *r_block = nullptr;
		unique_ptr<BufferHandle> l_block_handle;
		unique_ptr<BufferHandle> r_block_handle;
		data_ptr_t l_ptr;
		data_ptr_t r_ptr;

		RowDataBlock *result_block = &result->sorting_blocks.back();
		auto result_handle = buffer_manager.Pin(result_block->block);
		data_ptr_t result_ptr = result_handle->Ptr() + result_block->count * sorting_state.entry_size;

		idx_t copied = 0;
		while (copied < count) {
			// pin the blocks
			if (left.block_idx < left.sorting_blocks.size()) {
				l_block = &left.sorting_blocks[left.block_idx];
				l_block_handle = buffer_manager.Pin(l_block->block);
				l_ptr = l_block_handle->Ptr() + left.entry_idx * sorting_state.entry_size;
			}
			if (right.block_idx < right.sorting_blocks.size()) {
				r_block = &right.sorting_blocks[right.block_idx];
				r_block_handle = buffer_manager.Pin(r_block->block);
				r_ptr = r_block_handle->Ptr() + right.entry_idx * sorting_state.entry_size;
			}
			const idx_t &l_count = l_block ? l_block->count : 0;
			const idx_t &r_count = r_block ? r_block->count : 0;

			// create new result block (if needed)
			if (result_block->count == result_block->capacity) {
				result->CreateBlock();
				result_block = &result->sorting_blocks.back();
				result_handle = buffer_manager.Pin(result_block->block);
				result_ptr = result_handle->Ptr();
			}
			// copy using computed merge
			if (left.block_idx < left.sorting_blocks.size() && right.block_idx < right.sorting_blocks.size()) {
				// neither left nor right side is exhausted
				MergeConstantSize(l_ptr, left.entry_idx, l_count, r_ptr, right.entry_idx, r_count, result_block,
				                  result_ptr, sorting_state.entry_size, left_smaller, copied, count);
			} else if (right.block_idx == right.sorting_blocks.size()) {
				// right side is exhausted
				FlushConstantSize(l_ptr, left.entry_idx, l_count, result_block, result_ptr, sorting_state.entry_size,
				                  copied, count);
			} else {
				// left side is exhausted
				FlushConstantSize(r_ptr, right.entry_idx, r_count, result_block, result_ptr, sorting_state.entry_size,
				                  copied, count);
			}
			// move to the next block (if needed)
			if (left.block_idx < left.sorting_blocks.size() &&
			    left.entry_idx == left.sorting_blocks[left.block_idx].count) {
				left.block_idx++;
				left.entry_idx = 0;
			}
			if (right.block_idx < right.sorting_blocks.size() &&
			    right.entry_idx == right.sorting_blocks[right.block_idx].count) {
				right.block_idx++;
				right.entry_idx = 0;
			}
		}
	}

	//! Merges fixed/variable size SortedData
	void Merge(SortedData &result_data, SortedData &l_data, SortedData &r_data, const idx_t &count,
	           const bool *left_smaller, idx_t *next_entry_sizes) {
		// these are always used
		RowDataBlock *l_data_block = nullptr;
		RowDataBlock *r_data_block = nullptr;
		unique_ptr<BufferHandle> l_data_block_handle;
		unique_ptr<BufferHandle> r_data_block_handle;

		// these are only used when payload size is variable
		RowDataBlock *l_offset_block = nullptr;
		RowDataBlock *r_offset_block = nullptr;
		unique_ptr<BufferHandle> l_offset_block_handle;
		unique_ptr<BufferHandle> r_offset_block_handle;
		idx_t *l_offsets;
		idx_t *r_offsets;
		bool result_block_full = false;

		// result to write to
		RowDataBlock *result_data_block = &result_data.data_blocks.back();
		auto result_data_handle = buffer_manager.Pin(result_data_block->block);
		data_ptr_t result_ptr = result_data_handle->Ptr();
		result_ptr += result_data.constant_size ? result_data_block->count * result_data_block->entry_size
		                                        : result_data_block->byte_offset;

		RowDataBlock *result_offset_block;
		unique_ptr<BufferHandle> result_offset_handle;
		idx_t *result_offsets;
		if (!result_data.constant_size) {
			result_offset_block = &result_data.offset_blocks.back();
			result_offset_handle = buffer_manager.Pin(result_offset_block->block);
			result_offsets = (idx_t *)result_offset_handle->Ptr();
			result_offsets[0] = 0;
		}

		idx_t copied = 0;
		while (copied < count) {
			// pin the blocks
			if (l_data.block_idx < l_data.data_blocks.size()) {
				l_data_block = &l_data.data_blocks[l_data.block_idx];
				l_data_block_handle = buffer_manager.Pin(l_data_block->block);
			}
			if (r_data.block_idx < r_data.data_blocks.size()) {
				r_data_block = &r_data.data_blocks[r_data.block_idx];
				r_data_block_handle = buffer_manager.Pin(r_data_block->block);
			}
			const idx_t &l_count = l_data_block ? l_data_block->count : 0;
			const idx_t &r_count = r_data_block ? r_data_block->count : 0;
			// now copy
			if (result_data.constant_size) {
				const idx_t entry_size = result_data.entry_size;
				data_ptr_t l_ptr = l_data_block ? l_data_block_handle->Ptr() + l_data.entry_idx * entry_size : nullptr;
				data_ptr_t r_ptr = r_data_block ? r_data_block_handle->Ptr() + r_data.entry_idx * entry_size : nullptr;
				// create new result block (if needed)
				if (result_data_block->count == result_data_block->capacity) {
					result_data.CreateBlock();
					result_data_block = &result_data.data_blocks.back();
					result_data_handle = buffer_manager.Pin(result_data_block->block);
					result_ptr = result_data_handle->Ptr();
				}
				if (l_data.block_idx < l_data.data_blocks.size() && r_data.block_idx < r_data.data_blocks.size()) {
					// neither left nor right side is exhausted
					MergeConstantSize(l_ptr, l_data.entry_idx, l_count, r_ptr, r_data.entry_idx, r_count,
					                  result_data_block, result_ptr, entry_size, left_smaller, copied, count);
				} else if (r_data.block_idx == r_data.data_blocks.size()) {
					// right side is exhausted
					FlushConstantSize(l_ptr, l_data.entry_idx, l_count, result_data_block, result_ptr, entry_size,
					                  copied, count);
				} else {
					// left side is exhausted
					FlushConstantSize(r_ptr, r_data.entry_idx, r_count, result_data_block, result_ptr, entry_size,
					                  copied, count);
				}
			} else {
				// pin the offset blocks
				if (l_data.block_idx < l_data.offset_blocks.size()) {
					l_offset_block = &l_data.offset_blocks[l_data.block_idx];
					l_offset_block_handle = buffer_manager.Pin(l_offset_block->block);
					l_offsets = (idx_t *)l_offset_block_handle->Ptr();
				}
				if (r_data.block_idx < r_data.offset_blocks.size()) {
					r_offset_block = &r_data.offset_blocks[r_data.block_idx];
					r_offset_block_handle = buffer_manager.Pin(r_offset_block->block);
					r_offsets = (idx_t *)r_offset_block_handle->Ptr();
				}

				// create new result block (if needed)
				if (result_block_full || result_offset_block->count == result_offset_block->capacity) {
					result_data.CreateBlock();

					result_data_block = &result_data.data_blocks.back();
					result_data_handle = buffer_manager.Pin(result_data_block->block);
					result_ptr = result_data_handle->Ptr();

					result_offset_block = &result_data.offset_blocks.back();
					result_offset_handle = buffer_manager.Pin(result_offset_block->block);
					result_offsets = (idx_t *)result_offset_handle->Ptr();
					result_offsets[0] = 0;

					result_block_full = false;
				}

				data_ptr_t l_ptr = l_data_block ? l_data_block_handle->Ptr() + l_offsets[l_data.entry_idx] : nullptr;
				data_ptr_t r_ptr = r_data_block ? r_data_block_handle->Ptr() + r_offsets[r_data.entry_idx] : nullptr;
				if (l_data.block_idx < l_data.data_blocks.size() && r_data.block_idx < r_data.data_blocks.size()) {
					const idx_t result_byte_capacity = result_data_block->capacity * result_data_block->entry_size;
					const idx_t &result_offset_count = result_offset_block->count;
					const idx_t &result_offset_capacity = result_offset_block->capacity;
					// compute entry sizes, and how many entries we can fit until a next block must be fetched
					idx_t next;
					idx_t copy_bytes = 0;
					idx_t l_entry_idx_temp = l_data.entry_idx;
					idx_t r_entry_idx_temp = r_data.entry_idx;
					for (next = 0; copied + next < count && l_entry_idx_temp < l_count && r_entry_idx_temp < r_count &&
					               result_offset_count + next < result_offset_capacity;
					     next++) {
						const bool &l_smaller = left_smaller[copied + next];
						const bool r_smaller = !l_smaller;
						idx_t l_size = (l_offsets[l_entry_idx_temp + 1] - l_offsets[l_entry_idx_temp]);
						idx_t r_size = (r_offsets[r_entry_idx_temp + 1] - r_offsets[r_entry_idx_temp]);
						idx_t entry_size = l_smaller * l_size + r_smaller * r_size;
						if (result_data_block->byte_offset + copy_bytes + entry_size > result_byte_capacity) {
							result_block_full = true;
							break;
						}
						l_entry_idx_temp += l_smaller;
						r_entry_idx_temp += r_smaller;
						copy_bytes += entry_size;
						next_entry_sizes[copied + next] = entry_size;
					}
					// now we copy
					const idx_t const_next = next;
					for (next = 0; next < const_next; next++) {
						const bool &l_smaller = left_smaller[copied + next];
						const bool r_smaller = !l_smaller;
						const idx_t &entry_size = next_entry_sizes[copied + next];
						// use comparison bool (0 or 1) to copy an entry from either side
						memcpy(result_ptr, l_ptr, l_smaller * entry_size);
						memcpy(result_ptr, r_ptr, r_smaller * entry_size);
						result_ptr += entry_size;
						// use the comparison bool to increment entries and pointers
						l_data.entry_idx += l_smaller;
						r_data.entry_idx += r_smaller;
						l_ptr += l_smaller * entry_size;
						r_ptr += r_smaller * entry_size;
						// store offset
						result_offsets[result_offset_count + next + 1] =
						    result_offsets[result_offset_count + next] + entry_size;
					}
					result_data_block->byte_offset += copy_bytes;
					result_offset_block->count += next;
					result_data_block->count += next;
					copied += next;
				} else if (r_data.block_idx == r_data.data_blocks.size()) {
					// right side is exhausted
					FlushVariableSize(l_ptr, l_offsets, l_data.entry_idx, l_count, result_data_block, result_ptr,
					                  result_offset_block, result_offsets, copied, count, next_entry_sizes,
					                  result_block_full);
				} else {
					// left side is exhausted
					FlushVariableSize(r_ptr, r_offsets, r_data.entry_idx, r_count, result_data_block, result_ptr,
					                  result_offset_block, result_offsets, copied, count, next_entry_sizes,
					                  result_block_full);
				}
			}
			// move to new blocks (if needed)
			if (l_data.block_idx < l_data.data_blocks.size() &&
			    l_data.entry_idx == l_data.data_blocks[l_data.block_idx].count) {
				l_data.block_idx++;
				l_data.entry_idx = 0;
			}
			if (r_data.block_idx < r_data.data_blocks.size() &&
			    r_data.entry_idx == r_data.data_blocks[r_data.block_idx].count) {
				r_data.block_idx++;
				r_data.entry_idx = 0;
			}
		}
		if (!result_data.constant_size) {
			D_ASSERT(result_data.data_blocks.size() == result_data.offset_blocks.size());
		}
	}

	void MergeConstantSize(data_ptr_t &l_ptr, idx_t &l_entry_idx, const idx_t &l_count, data_ptr_t &r_ptr,
	                       idx_t &r_entry_idx, const idx_t &r_count, RowDataBlock *target_block, data_ptr_t &target_ptr,
	                       const idx_t &entry_size, const bool left_smaller[], idx_t &copied, const idx_t &count) {
		const idx_t next = MinValue(count - copied, target_block->capacity - target_block->count);
		idx_t i;
		for (i = 0; i < next && l_entry_idx < l_count && r_entry_idx < r_count; i++) {
			const bool &l_smaller = left_smaller[copied + i];
			const bool r_smaller = !l_smaller;
			// use comparison bool (0 or 1) to copy an entry from either side
			memcpy(target_ptr, l_ptr, l_smaller * entry_size);
			memcpy(target_ptr, r_ptr, r_smaller * entry_size);
			target_ptr += entry_size;
			// use the comparison bool to increment entries and pointers
			l_entry_idx += l_smaller;
			r_entry_idx += r_smaller;
			l_ptr += l_smaller * entry_size;
			r_ptr += r_smaller * entry_size;
		}
		// update counts
		target_block->count += i;
		copied += i;
	}

	void FlushConstantSize(data_ptr_t &source_ptr, idx_t &source_entry_idx, const idx_t &source_count,
	                       RowDataBlock *target_block, data_ptr_t &target_ptr, const idx_t &entry_size, idx_t &copied,
	                       const idx_t &count) {
		// compute how many entries we can fit
		idx_t next = MinValue(count - copied, target_block->capacity - target_block->count);
		next = MinValue(next, source_count - source_entry_idx);
		// copy them all in a single memcpy
		const idx_t copy_bytes = next * entry_size;
		memcpy(target_ptr, source_ptr, copy_bytes);
		target_ptr += copy_bytes;
		source_ptr += copy_bytes;
		// update counts
		source_entry_idx += next;
		target_block->count += next;
		copied += next;
	}

	void FlushVariableSize(data_ptr_t &source_ptr, idx_t source_offsets[], idx_t &source_entry_idx,
	                       const idx_t &source_count, RowDataBlock *target_data_block, data_ptr_t &target_ptr,
	                       RowDataBlock *target_offset_block, idx_t target_offsets[], idx_t &copied, const idx_t &count,
	                       idx_t next_entry_sizes[], bool &result_block_full) {
		const idx_t &target_byte_offset = target_data_block->byte_offset;
		const idx_t target_byte_capacity = target_data_block->capacity * target_data_block->entry_size;
		const idx_t &target_offset_count = target_offset_block->count;
		const idx_t &target_offset_capacity = target_offset_block->capacity;
		// compute entry sizes
		idx_t next;
		idx_t copy_bytes = 0;
		for (next = 0; copied + next < count && source_entry_idx + next < source_count &&
		               target_offset_count + next < target_offset_capacity;
		     next++) {
			idx_t entry_size = (source_offsets[source_entry_idx + 1] - source_offsets[source_entry_idx]);
			if (target_byte_offset + copy_bytes + entry_size > target_byte_capacity) {
				result_block_full = true;
				break;
			}
			source_entry_idx++;
			copy_bytes += entry_size;
			next_entry_sizes[copied + next] = entry_size;
		}
		// copy them all in a single memcpy
		memcpy(target_ptr, source_ptr, copy_bytes);
		source_ptr += copy_bytes;
		target_ptr += copy_bytes;
		// and set the offsets
		const idx_t &const_next = next;
		for (idx_t i = 0; i < const_next; i++) {
			const idx_t &entry_size = next_entry_sizes[copied + i];
			target_offsets[target_offset_count + i + 1] = target_offsets[target_offset_count + i] + entry_size;
		}
		// update counts
		target_data_block->byte_offset += copy_bytes;
		target_offset_block->count += next;
		target_data_block->count += next;
		copied += next;
	}

private:
	Pipeline &parent;
	ClientContext &context;
	BufferManager &buffer_manager;
	OrderGlobalState &state;
	const SortingState &sorting_state;
	const PayloadState &payload_state;

	SortedBlock &left;
	SortedBlock &right;
	unique_ptr<SortedBlock> result;
};

void PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state_p) {
	this->sink_state = move(state_p);
	auto &state = (OrderGlobalState &)*this->sink_state;
	if (state.sorted_blocks.empty()) {
		return;
	}
	// compute total count
	for (auto &sb : state.sorted_blocks) {
		state.total_count += sb->sorting_blocks.back().count;
	}

	// use the data that we have to determine which block sizes to use during the merge
	const auto &sorting_state = *state.sorting_state;
	for (auto &sb : state.sorted_blocks) {
		auto &block = sb->sorting_blocks.back();
		state.sorting_block_capacity = MaxValue(state.sorting_block_capacity, block.capacity);
	}

	if (!sorting_state.all_constant) {
		const idx_t num_cols = sorting_state.num_cols;
		state.var_sorting_data_block_dims = vector<std::pair<idx_t, idx_t>>(num_cols, std::make_pair(0, 0));
		state.var_sorting_offset_block_capacity = vector<idx_t>(num_cols, 0);
		for (idx_t col_idx = 0; col_idx < num_cols; col_idx++) {
			idx_t &var_data_capacity = state.var_sorting_data_block_dims[col_idx].first;
			idx_t &var_data_entry_size = state.var_sorting_data_block_dims[col_idx].second;
			idx_t &var_offset_capacity = state.var_sorting_offset_block_capacity[col_idx];
			if (!sorting_state.constant_size[col_idx]) {
				for (auto &sb : state.sorted_blocks) {
					const auto &data_block = sb->var_sorting_cols[col_idx]->data_blocks.back();
					var_data_capacity = MaxValue(var_data_capacity, data_block.capacity);
					var_data_entry_size = MaxValue(var_data_entry_size, data_block.entry_size);
					const auto &offset_block = sb->var_sorting_cols[col_idx]->offset_blocks.back();
					var_offset_capacity = MaxValue(var_offset_capacity, offset_block.capacity);
				}
			}
		}
	}

	const auto &payload_state = *state.payload_state;
	idx_t &payload_capacity = state.payload_data_block_dims.first;
	idx_t &payload_entry_size = state.payload_data_block_dims.second;
	for (auto &sb : state.sorted_blocks) {
		auto &data_block = sb->payload_data->data_blocks.back();
		payload_capacity = MaxValue(payload_capacity, data_block.capacity);
		payload_entry_size = data_block.entry_size;
		if (!payload_state.all_constant) {
			auto &offset_block = sb->payload_data->offset_blocks.back();
			state.payload_offset_block_capacity = MaxValue(state.payload_offset_block_capacity, offset_block.capacity);
		}
	}

	if (state.sorted_blocks.size() > 1) {
		// more than one block - merge
		PhysicalOrder::ScheduleMergeTasks(pipeline, context, state);
	} else {
		// clean up sorting data - payload is sorted
		for (auto &sb : state.sorted_blocks) {
			sb->UnregisterSortingBlocks();
		}
	}
}

void PhysicalOrder::ScheduleMergeTasks(Pipeline &pipeline, ClientContext &context, OrderGlobalState &state) {
	if (state.sorted_blocks.size() == 1) {
		for (auto &sb : state.sorted_blocks) {
			sb->UnregisterSortingBlocks();
		}
		pipeline.Finish();
		return;
	}

	// uneven amount of blocks
	auto num_blocks = state.sorted_blocks.size();
	if (num_blocks % 2 == 1) {
		state.sorted_blocks_temp.push_back(move(state.sorted_blocks.back()));
		state.sorted_blocks.pop_back();
		num_blocks--;
	}

	auto &ts = TaskScheduler::GetScheduler(context);
	pipeline.total_tasks += num_blocks / 2;
	for (idx_t i = 0; i < num_blocks; i += 2) {
		auto new_task = make_unique<PhysicalOrderMergeTask>(pipeline, context, state, *state.sorted_blocks[i],
		                                                    *state.sorted_blocks[i + 1]);
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

	SortedData *payload_data;

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

static void UpdateStateIndices(const idx_t &next, const SortedData &payload_data, OrderParallelState &parallel_state) {
	parallel_state.global_entry_idx += next;

	idx_t remaining = next;
	while (remaining > 0) {
		auto &data_block = payload_data.data_blocks[parallel_state.data_block_idx];
		if (parallel_state.data_entry_idx + remaining > data_block.count) {
			remaining -= data_block.count - parallel_state.data_entry_idx;
			parallel_state.data_block_idx++;
			parallel_state.data_entry_idx = 0;
		} else {
			parallel_state.data_entry_idx += remaining;
			remaining = 0;
		}
	}

	if (!payload_data.constant_size) {
		remaining = next;
		while (remaining > 0) {
			auto &offset_block = payload_data.offset_blocks[parallel_state.offset_block_idx];
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
	auto &payload_data = *state.payload_data;

	vector<unique_ptr<BufferHandle>> handles;

	// set up a batch of pointers to scan data from
	idx_t count = 0;
	while (count < scan_count) {
		auto &data_block = payload_data.data_blocks[state.data_block_idx];
		auto data_handle = buffer_manager.Pin(data_block.block);
		idx_t next = MinValue(data_block.count - state.data_entry_idx, scan_count - count);
		if (payload_state.all_constant) {
			data_ptr_t payl_dataptr = data_handle->Ptr() + state.data_entry_idx * payload_state.entry_size;
			for (idx_t i = 0; i < next; i++) {
				state.validitymask_locations[count + i] = payl_dataptr;
				state.key_locations[count + i] = payl_dataptr + payload_state.validitymask_size;
				payl_dataptr += payload_state.entry_size;
			}
		} else {
			auto &offset_block = payload_data.offset_blocks[state.offset_block_idx];
			auto offset_handle = buffer_manager.Pin(offset_block.block);
			next = MinValue(next, offset_block.count - state.offset_entry_idx);

			const data_ptr_t payl_dataptr = data_handle->Ptr();
			const idx_t *offsets = (idx_t *)offset_handle->Ptr();
			for (idx_t i = 0; i < next; i++) {
				state.validitymask_locations[count + i] = payl_dataptr + offsets[state.offset_entry_idx + i];
				state.key_locations[count + i] =
				    state.validitymask_locations[count + i] + payload_state.validitymask_size;
			}

			state.offset_entry_idx += next;
			if (state.offset_entry_idx == offset_block.count) {
				state.offset_block_idx++;
				state.offset_entry_idx = 0;
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
		RowDataCollection::DeserializeIntoVector(chunk.data[payl_col], scan_count, payl_col, state.key_locations,
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
		state.payload_data = gstate.sorted_blocks.back()->payload_data.get();
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
				UpdateStateIndices(next, *state.payload_data, parallel_state);
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
