#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"

#include <numeric>

namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t estimated_cardinality)
    : PhysicalSink(PhysicalOperatorType::ORDER_BY, move(types), estimated_cardinality), orders(move(orders)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct SortingState {
	explicit SortingState(const vector<BoundOrderByNode> &orders)
	    : column_count(orders.size()), all_constant(true), comparison_size(0), entry_size(0) {
		vector<LogicalType> blob_layout_types;
		for (idx_t i = 0; i < orders.size(); i++) {
			const auto &order = orders[i];

			order_types.push_back(order.type);
			order_by_null_types.push_back(order.null_order);
			auto &expr = *order.expression;
			logical_types.push_back(expr.return_type);

			auto physical_type = expr.return_type.InternalType();
			all_constant = all_constant && TypeIsConstantSize(physical_type);
			constant_size.push_back(TypeIsConstantSize(physical_type));
			column_sizes.push_back(0);
			auto &col_size = column_sizes.back();

			stats.push_back(expr.stats ? expr.stats.get() : nullptr);
			has_null.push_back(true); // TODO: make use of statistics

			col_size += has_null.back() ? 1 : 0;
			if (TypeIsConstantSize(physical_type)) {
				col_size += GetTypeIdSize(physical_type);
			} else {
				switch (physical_type) {
				case PhysicalType::VARCHAR:
					// TODO: make use of statistics
					col_size += string_t::INLINE_LENGTH;
					break;
				default:
					throw NotImplementedException("Unable to order column with type %s", expr.return_type.ToString());
				}
				sorting_to_blob_col[i] = blob_layout_types.size();
				blob_layout_types.push_back(expr.return_type);
			}
			comparison_size += col_size;
		}
		entry_size = comparison_size + sizeof(idx_t);
		blob_layout.Initialize(blob_layout_types);
	}

	idx_t column_count;
	vector<OrderType> order_types;
	vector<OrderByNullType> order_by_null_types;
	vector<LogicalType> logical_types;

	bool all_constant;
	vector<bool> constant_size;
	vector<idx_t> column_sizes;
	vector<BaseStatistics *> stats;
	vector<bool> has_null;

	idx_t comparison_size;
	idx_t entry_size;

	RowLayout blob_layout;
	unordered_map<idx_t, idx_t> sorting_to_blob_col;
};

class OrderGlobalState : public GlobalOperatorState {
public:
	OrderGlobalState(SortingState sorting_state, RowLayout payload_layout)
	    : sorting_state(move(sorting_state)), payload_layout(move(payload_layout)), next_heap_block_id(0),
	      total_count(0), block_capacity(0), sorting_heap_capacity(0), payload_heap_capacity(0), external(false) {
	}

	~OrderGlobalState() override;

	//! The lock for updating the order global state
	std::mutex lock;
	//! Constants concerning sorting and payload data
	const SortingState sorting_state;
	const RowLayout payload_layout;

	//! Sorted data
	vector<unique_ptr<SortedBlock>> sorted_blocks;
	vector<vector<unique_ptr<SortedBlock>>> sorted_blocks_temp;
	unique_ptr<SortedBlock> odd_one_out = nullptr;
	//! Pinned heap data (if sorting in memory)
	vector<shared_ptr<BlockHandle>> heap_blocks;
	vector<unique_ptr<BufferHandle>> pinned_blocks;
	//! Next heap block id (used if doing external sort)
	std::atomic<uint32_t> next_heap_block_id;

	//! Total count - set after PhysicalOrder::Finalize is called
	idx_t total_count;
	//! Capacity (number of rows) used to initialize blocks
	idx_t block_capacity;
	//! Capacity (number of bytes) used to initialize blocks
	idx_t sorting_heap_capacity;
	idx_t payload_heap_capacity;

	//! Whether we are doing an external sort
	bool external;
	//! Progress in merge path stage
	idx_t pair_idx;
	idx_t l_start;
	idx_t r_start;
};

class OrderLocalState : public LocalSinkState {
public:
	OrderLocalState() : initialized(false) {
	}

	//! Whether this local state has been initialized
	bool initialized;
	//! Local copy of the sorting expression executor
	ExpressionExecutor executor;
	//! Holds a vector of incoming sorting columns
	DataChunk sort;

	//! Initialize the local state using the global state
	void Initialize(ClientContext &context, OrderGlobalState &gstate) {
		auto &buffer_manager = BufferManager::GetBufferManager(context);
		auto &sorting_state = gstate.sorting_state;
		auto &payload_layout = gstate.payload_layout;
		// Radix sorting data
		idx_t vectors_per_block =
		    (Storage::BLOCK_ALLOC_SIZE / sorting_state.entry_size + STANDARD_VECTOR_SIZE) / STANDARD_VECTOR_SIZE;
		sorting_data_radix = make_unique<RowDataCollection>(buffer_manager, vectors_per_block * STANDARD_VECTOR_SIZE,
		                                                    sorting_state.entry_size);
		// Blob sorting data
		if (!sorting_state.all_constant) {
			auto blob_row_width = sorting_state.blob_layout.GetRowWidth();
			vectors_per_block =
			    (Storage::BLOCK_ALLOC_SIZE / blob_row_width + STANDARD_VECTOR_SIZE) / STANDARD_VECTOR_SIZE;
			sorting_data_blob = make_unique<RowDataCollection>(buffer_manager, vectors_per_block * STANDARD_VECTOR_SIZE,
			                                                   blob_row_width);
			sorting_data_heap = make_unique<RowDataCollection>(buffer_manager, Storage::BLOCK_ALLOC_SIZE / 8, 8, true);
		}
		// Payload data
		auto payload_row_width = payload_layout.GetRowWidth();
		vectors_per_block =
		    (Storage::BLOCK_ALLOC_SIZE / payload_row_width + STANDARD_VECTOR_SIZE) / STANDARD_VECTOR_SIZE;
		payload_data =
		    make_unique<RowDataCollection>(buffer_manager, vectors_per_block * STANDARD_VECTOR_SIZE, payload_row_width);
		payload_heap = make_unique<RowDataCollection>(buffer_manager, Storage::BLOCK_ALLOC_SIZE / 8, 8, true);
		// Init done
		initialized = true;
	}

	//! Whether the localstate has collected enough data to perform an external sort
	bool Full(ClientContext &context, const SortingState &sorting_state, const RowLayout &payload_layout) {
		// Compute the size of the collected data (in bytes)
		idx_t size_in_bytes = sorting_data_radix->count * sorting_state.entry_size;
		if (!sorting_state.all_constant) {
			size_in_bytes += sorting_data_blob->count * sorting_state.blob_layout.GetRowWidth();
			for (auto &block : sorting_data_heap->blocks) {
				size_in_bytes += block.byte_offset;
			}
		}
		size_in_bytes += payload_data->count * payload_layout.GetRowWidth();
		if (!payload_layout.AllConstant()) {
			for (auto &block : payload_data->blocks) {
				size_in_bytes += block.byte_offset;
			}
		}
		// Get the max memory and number of threads
		auto &buffer_manager = BufferManager::GetBufferManager(context);
		auto &task_scheduler = TaskScheduler::GetScheduler(context);
		idx_t max_memory = buffer_manager.GetMaxMemory();
		idx_t num_threads = task_scheduler.NumberOfThreads();
		// memory usage per thread should scale with max mem / num threads
		// we take 10% of the max memory, to be VERY conservative
		// TODO: make sure heap blocks are never bigger than 4GB
		return size_in_bytes > (0.1 * max_memory / num_threads);
	}

	//! Radix/memcmp sortable data
	unique_ptr<RowDataCollection> sorting_data_radix;
	//! Variable sized sorting data and accompanying heap
	unique_ptr<RowDataCollection> sorting_data_blob;
	unique_ptr<RowDataCollection> sorting_data_heap;
	//! Payload data and accompanying heap
	unique_ptr<RowDataCollection> payload_data;
	unique_ptr<RowDataCollection> payload_heap;
	//! Sorted data
	vector<unique_ptr<SortedBlock>> sorted_blocks;
	//! Constant buffers allocated for vector serialization
	const SelectionVector &sel_ptr = FlatVector::INCREMENTAL_SELECTION_VECTOR;
	Vector addresses = Vector(LogicalType::POINTER);
	idx_t entry_sizes[STANDARD_VECTOR_SIZE];
};

unique_ptr<GlobalOperatorState> PhysicalOrder::GetGlobalState(ClientContext &context) {
	RowLayout payload_layout;
	payload_layout.Initialize(types);
	auto state = make_unique<OrderGlobalState>(SortingState(orders), payload_layout);
	state->external = context.force_external;
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
                         DataChunk &input) const {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;
	const auto &sorting_state = gstate.sorting_state;
	const auto &payload_layout = gstate.payload_layout;

	if (!lstate.initialized) {
		lstate.Initialize(context.client, gstate);
	}

	// obtain sorting columns
	auto &sort = lstate.sort;
	lstate.executor.Execute(input, sort);

	// build and serialize sorting data
	auto data_pointers = FlatVector::GetData<data_ptr_t>(lstate.addresses);
	lstate.sorting_data_radix->Build(sort.size(), data_pointers, nullptr);
	for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
		bool has_null = sorting_state.has_null[sort_col];
		bool nulls_first = sorting_state.order_by_null_types[sort_col] == OrderByNullType::NULLS_FIRST;
		bool desc = sorting_state.order_types[sort_col] == OrderType::DESCENDING;
		// TODO: use actual string statistics
		lstate.sorting_data_radix->SerializeVectorSortable(sort.data[sort_col], sort.size(), lstate.sel_ptr,
		                                                   sort.size(), data_pointers, desc, has_null, nulls_first,
		                                                   string_t::INLINE_LENGTH);
	}

	// also fully serialize variable size sorting columns
	if (!sorting_state.all_constant) {
		DataChunk blob_chunk;
		blob_chunk.SetCardinality(sort.size());
		for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
			if (!TypeIsConstantSize(sort.data[sort_col].GetType().InternalType())) {
				blob_chunk.data.emplace_back(sort.data[sort_col]);
			}
		}
		lstate.sorting_data_blob->Build(blob_chunk.size(), data_pointers, nullptr);
		auto blob_data = blob_chunk.Orrify();
		RowOperations::Scatter(blob_chunk, blob_data.get(), sorting_state.blob_layout, lstate.addresses,
		                       *lstate.sorting_data_heap, lstate.sel_ptr, blob_chunk.size());
	}

	lstate.payload_data->Build(input.size(), data_pointers, nullptr);
	auto input_data = input.Orrify();
	RowOperations::Scatter(input, input_data.get(), payload_layout, lstate.addresses, *lstate.payload_heap,
	                       lstate.sel_ptr, input.size());

	// when sorting data reaches a certain size, we sort it
	if (lstate.Full(context.client, sorting_state, payload_layout)) {
		gstate.external = true;
		SortLocalState(context.client, lstate, gstate);
	}
}

void PhysicalOrder::Combine(ExecutionContext &context, GlobalOperatorState &gstate_p, LocalSinkState &lstate_p) {
	auto &gstate = (OrderGlobalState &)gstate_p;
	auto &lstate = (OrderLocalState &)lstate_p;
	if (!lstate.sorting_data_radix) {
		return;
	}

	SortLocalState(context.client, lstate, gstate);

	lock_guard<mutex> append_lock(gstate.lock);
	for (auto &cb : lstate.sorted_blocks) {
		gstate.sorted_blocks.push_back(move(cb));
	}
}

template <class SORTED>
void TemplatedGlobalToLocalIndex(SORTED &sorted, const vector<RowDataBlock> &blocks, const idx_t &global_idx,
                                 idx_t &local_block_index, idx_t &local_entry_index) {
	if (global_idx == sorted.Count()) {
		local_block_index = blocks.size() - 1;
		local_entry_index = blocks.back().count;
		return;
	}
	D_ASSERT(global_idx < sorted.Count());
	local_entry_index = global_idx;
	for (local_block_index = 0; local_block_index < blocks.size(); local_block_index++) {
		const idx_t &block_count = blocks[local_block_index].count;
		if (local_entry_index >= block_count) {
			local_entry_index -= block_count;
		} else {
			break;
		}
	}
	D_ASSERT(local_entry_index < blocks[local_block_index].count);
}

struct SortedData {
public:
	SortedData(const RowLayout &layout, BufferManager &buffer_manager, OrderGlobalState &state)
	    : layout(layout), block_idx(0), entry_idx(0), buffer_manager(buffer_manager), state(state) {
	}

	idx_t Count() {
		idx_t count = std::accumulate(data_blocks.begin(), data_blocks.end(), 0,
		                              [](idx_t a, const RowDataBlock &b) { return a + b.count; });
		return count;
	}

	//! Initialize this to write data to during the merge
	void InitializeWrite(const idx_t &heap_capacity_p) {
		heap_capacity = heap_capacity_p;
		CreateDataBlock();
		if (!layout.AllConstant() && state.external) {
			CreateHeapBlock();
		}
	}

	//! Initialize new block to write to
	void CreateDataBlock() {
		data_blocks.emplace_back(buffer_manager, state.block_capacity, layout.GetRowWidth());
	}

	void CreateHeapBlock() {
		D_ASSERT(!layout.AllConstant());
		heap_block_id = state.next_heap_block_id++;
		heap_blocks.emplace_back(buffer_manager, heap_capacity / 8, 8);
		heap_blocks.back().heap_block_id = heap_block_id;
	}

	idx_t GetHeapBlockIndex(uint32_t block_id) {
		for (idx_t i = 0; i < heap_blocks.size(); i++) {
			if (heap_blocks[i].heap_block_id == block_id) {
				return i;
			}
		}
		D_ASSERT(false);
		return 0;
	}

	inline data_ptr_t DataPtr() const {
		D_ASSERT(data_blocks[block_idx].block->Readers() != 0 &&
		         data_handle->handle->BlockId() == data_blocks[block_idx].block->BlockId());
		return data_ptr + entry_idx * layout.GetRowWidth();
	}

	inline data_ptr_t HeapPtr() const {
		D_ASSERT(!layout.AllConstant() && state.external);
		return heap_ptr + Load<uint32_t>(data_ptr + layout.GetHeapOffsetOffset());
	}

	void Pin() {
		PinData();
		if (!layout.AllConstant() && state.external) {
			PinHeap();
		}
	}

	inline void Advance() {
		entry_idx++;
		if (entry_idx == data_blocks[block_idx].count) {
			block_idx++;
			entry_idx = 0;
			if (block_idx < data_blocks.size()) {
				PinData();
			} else {
				UnpinAndReset(block_idx, entry_idx);
				return;
			}
		}
		if (!layout.AllConstant() && state.external) {
			D_ASSERT(block_idx < data_blocks.size());
			uint32_t block_id = Load<uint32_t>(data_ptr + layout.GetHeapBlockIdOffset());
			if (heap_block_id != block_id) {
				heap_block_id = block_id;
				heap_block_idx++;
				PinHeap();
			}
		}
	}

	void UnpinAndReset(idx_t block_idx_to, idx_t entry_idx_to) {
		data_handle = nullptr;
		heap_handle = nullptr;
		data_ptr = nullptr;
		heap_ptr = nullptr;
		block_idx = block_idx_to;
		entry_idx = entry_idx_to;
	}

	void GlobalToLocalIndex(const idx_t &global_idx, idx_t &local_block_index, idx_t &local_entry_index) {
		TemplatedGlobalToLocalIndex<SortedData>(*this, data_blocks, global_idx, local_block_index, local_entry_index);
	}

	unique_ptr<SortedData> CreateSlice(const idx_t start, const idx_t end) {
		// Identify blocks/entry indices of this slice
		idx_t start_block_index;
		idx_t start_entry_index;
		GlobalToLocalIndex(start, start_block_index, start_entry_index);
		idx_t end_block_index;
		idx_t end_entry_index;
		GlobalToLocalIndex(end, end_block_index, end_entry_index);
		// Add the corresponding blocks to the result
		auto result = make_unique<SortedData>(layout, buffer_manager, state);
		for (idx_t i = start_block_index; i <= end_block_index; i++) {
			result->data_blocks.push_back(data_blocks[i]);
		}
		// Use start and end entry indices to set the boundaries
		result->entry_idx = start_entry_index;
		D_ASSERT(end_entry_index <= result->data_blocks.back().count);
		result->data_blocks.back().count = end_entry_index;
		return result;
	}

	//! Layout of this data
	const RowLayout layout;
	//! Data and heap blocks
	vector<RowDataBlock> data_blocks;
	vector<RowDataBlock> heap_blocks;
	//! Read indices row data
	idx_t block_idx;
	idx_t entry_idx;
	//! Read indices heap data
	uint32_t heap_block_id;
	idx_t heap_block_idx;

private:
	void PinData() {
		D_ASSERT(block_idx < data_blocks.size());
		data_handle = buffer_manager.Pin(data_blocks[block_idx].block);
		data_ptr = data_handle->Ptr();
	}

	void PinHeap() {
		D_ASSERT(!layout.AllConstant() && state.external);
		heap_block_id = Load<uint32_t>(data_ptr + layout.GetHeapBlockIdOffset());
		heap_block_idx = GetHeapBlockIndex(heap_block_id);
		heap_handle = buffer_manager.Pin(heap_blocks[heap_block_idx].block);
		heap_ptr = heap_handle->Ptr();
	}

	//! The buffer manager
	BufferManager &buffer_manager;
	//! The global state
	OrderGlobalState &state;
	//! Buffer handles to the data being currently read
	unique_ptr<BufferHandle> data_handle;
	unique_ptr<BufferHandle> heap_handle;
	//! Pointers into the buffers being currently read
	data_ptr_t data_ptr;
	data_ptr_t heap_ptr;
	//! Capacity (in bytes) of the heap blocks
	idx_t heap_capacity;
};

struct SortedBlock {
public:
	SortedBlock(BufferManager &buffer_manager, OrderGlobalState &state)
	    : block_idx(0), entry_idx(0), buffer_manager(buffer_manager), state(state), sorting_state(state.sorting_state),
	      payload_layout(state.payload_layout) {
		sorting_data_blob = make_unique<SortedData>(sorting_state.blob_layout, buffer_manager, state);
		payload_data = make_unique<SortedData>(payload_layout, buffer_manager, state);
	}

	idx_t Count() {
		idx_t count = std::accumulate(sorting_data_radix.begin(), sorting_data_radix.end(), 0,
		                              [](idx_t a, const RowDataBlock &b) { return a + b.count; });
#ifdef DEBUG
		if (!sorting_state.all_constant) {
			D_ASSERT(count == sorting_data_blob->Count());
		}
#endif
		D_ASSERT(count == payload_data->Count());
		return count;
	}

	idx_t Remaining() {
		idx_t remaining = 0;
		if (block_idx < sorting_data_radix.size()) {
			remaining += sorting_data_radix[block_idx].count - entry_idx;
			for (idx_t i = block_idx + 1; i < sorting_data_radix.size(); i++) {
				remaining += sorting_data_radix[i].count;
			}
		}
		return remaining;
	}

	//! Initialize this block to write data to
	void InitializeWrite() {
		CreateBlock();
		if (!sorting_state.all_constant) {
			sorting_data_blob->InitializeWrite(state.sorting_heap_capacity);
		}
		payload_data->InitializeWrite(state.payload_heap_capacity);
	}

	//! Init new block to write to
	void CreateBlock() {
		sorting_data_radix.emplace_back(buffer_manager, state.block_capacity, sorting_state.entry_size);
	}
	//! Cleanup sorting data
	void UnregisterSortingBlocks() {
		for (auto &block : sorting_data_radix) {
			buffer_manager.UnregisterBlock(block.block->BlockId(), true);
		}
		if (!sorting_state.all_constant) {
			for (auto &block : sorting_data_blob->data_blocks) {
				buffer_manager.UnregisterBlock(block.block->BlockId(), true);
			}
			if (state.external) {
				for (auto &block : sorting_data_blob->heap_blocks) {
					buffer_manager.UnregisterBlock(block.block->BlockId(), true);
				}
			}
		}
	}
	//! Cleanup payload data
	void UnregisterPayloadBlocks() {
		for (auto &block : payload_data->data_blocks) {
			buffer_manager.UnregisterBlock(block.block->BlockId(), true);
		}
		if (state.external) {
			if (!payload_data->layout.AllConstant()) {
				for (auto &block : payload_data->heap_blocks) {
					buffer_manager.UnregisterBlock(block.block->BlockId(), true);
				}
			}
		}
	}

	void AppendSortedBlocks(vector<unique_ptr<SortedBlock>> &sorted_blocks) {
		D_ASSERT(Count() == 0);
		for (auto &sb : sorted_blocks) {
			for (auto &radix_block : sb->sorting_data_radix) {
				sorting_data_radix.push_back(move(radix_block));
			}
			if (!sorting_state.all_constant) {
				for (auto &blob_block : sb->sorting_data_blob->data_blocks) {
					sorting_data_blob->data_blocks.push_back(move(blob_block));
				}
				for (auto &heap_block : sb->sorting_data_blob->heap_blocks) {
					sorting_data_blob->heap_blocks.push_back(move(heap_block));
				}
			}
			for (auto &payload_data_block : sb->payload_data->data_blocks) {
				payload_data->data_blocks.push_back(move(payload_data_block));
			}
			if (!payload_data->layout.AllConstant()) {
				for (auto &payload_heap_block : sb->payload_data->heap_blocks) {
					payload_data->heap_blocks.push_back(move(payload_heap_block));
				}
			}
		}
	}

	void GlobalToLocalIndex(const idx_t &global_idx, idx_t &local_block_index, idx_t &local_entry_index) {
		TemplatedGlobalToLocalIndex<SortedBlock>(*this, sorting_data_radix, global_idx, local_block_index,
		                                         local_entry_index);
	}

	unique_ptr<SortedBlock> CreateSlice(const idx_t start, const idx_t end) {
		// identify blocks/entry indices of this slice
		idx_t start_block_index;
		idx_t start_entry_index;
		GlobalToLocalIndex(start, start_block_index, start_entry_index);
		idx_t end_block_index;
		idx_t end_entry_index;
		GlobalToLocalIndex(end, end_block_index, end_entry_index);
		// add the corresponding blocks to the result
		auto result = make_unique<SortedBlock>(buffer_manager, state);
		for (idx_t i = start_block_index; i <= end_block_index; i++) {
			result->sorting_data_radix.push_back(sorting_data_radix[i]);
		}
		// use start and end entry indices to set the boundaries
		result->entry_idx = start_entry_index;
		D_ASSERT(end_entry_index <= result->sorting_data_radix.back().count);
		result->sorting_data_radix.back().count = end_entry_index;
		// same for the var size sorting data
		if (!sorting_state.all_constant) {
			result->sorting_data_blob = sorting_data_blob->CreateSlice(start, end);
		}
		// and the payload data
		result->payload_data = payload_data->CreateSlice(start, end);
		D_ASSERT(result->Remaining() == end - start);
		return result;
	}

public:
	//! Radix/memcmp sortable data
	vector<RowDataBlock> sorting_data_radix;
	idx_t block_idx;
	idx_t entry_idx;
	//! Variable sized sorting data
	unique_ptr<SortedData> sorting_data_blob;
	//! Payload data
	unique_ptr<SortedData> payload_data;

private:
	//! Buffer manager, and sorting state constants
	BufferManager &buffer_manager;
	OrderGlobalState &state;
	const SortingState &sorting_state;
	const RowLayout &payload_layout;

	//! Handle and ptr for sorting_blocks
	unique_ptr<BufferHandle> sorting_handle;
};

OrderGlobalState::~OrderGlobalState() {
	std::lock_guard<mutex> glock(lock);
	for (auto &sb : sorted_blocks) {
		sb->UnregisterPayloadBlocks();
	}
	sorted_blocks.clear();
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

static inline bool IsValid(const idx_t &col_idx, const data_ptr_t row_ptr) {
	ValidityBytes row_mask(row_ptr);
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
	return row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry);
}

static inline bool TieIsBreakable(const idx_t &col_idx, const data_ptr_t row_ptr, const RowLayout &row_layout) {
	// Check if the blob is NULL
	if (!IsValid(col_idx, row_ptr)) {
		// Can't break a NULL tie
		return false;
	}
	// Check if it is too short
	switch (row_layout.GetTypes()[col_idx].InternalType()) {
	case PhysicalType::VARCHAR: {
		const auto &tie_col_offset = row_layout.GetOffsets()[col_idx];
		string_t tie_string = Load<string_t>(row_ptr + tie_col_offset);
		if (tie_string.GetSize() < string_t::INLINE_LENGTH) {
			// No need to break the tie
			return false;
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented compare for type %s", row_layout.GetTypes()[col_idx].ToString());
	}
	return true;
}

template <class T>
static inline int TemplatedCompareVal(const T &left_val, const T &right_val, const int &order) {
	int comp_res;
	if (Equals::Operation<T>(left_val, right_val)) {
		comp_res = 0;
	} else if (LessThan::Operation<T>(left_val, right_val)) {
		comp_res = -1;
	} else {
		comp_res = 1;
	}
	return order * comp_res;
}

static inline int CompareVal(const data_ptr_t l_ptr, const data_ptr_t r_ptr, const LogicalType &type,
                             const int &order) {
	switch (type.InternalType()) {
	case PhysicalType::VARCHAR: {
		string_t left_val = Load<string_t>(l_ptr);
		string_t right_val = Load<string_t>(r_ptr);
		return TemplatedCompareVal<string_t>(left_val, right_val, order);
	}
	default:
		throw NotImplementedException("Unimplemented CompareVal for type %s", type.ToString());
	}
}

static inline void UnswizzleSingleValue(data_ptr_t data_ptr, const data_ptr_t &heap_ptr, const LogicalType &type) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data_ptr += sizeof(uint32_t);
	}
	Store<data_ptr_t>(heap_ptr + Load<idx_t>(data_ptr), data_ptr);
}

static inline void SwizzleSingleValue(data_ptr_t data_ptr, const data_ptr_t &heap_ptr, const LogicalType &type) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data_ptr += sizeof(uint32_t);
	}
	Store<idx_t>(Load<data_ptr_t>(data_ptr) - heap_ptr, data_ptr);
}

static inline int BreakBlobTie(const idx_t &tie_col, const SortedData &left, const SortedData &right,
                               const SortingState &sorting_state, const bool &external) {
	const idx_t &col_idx = sorting_state.sorting_to_blob_col.at(tie_col);
	data_ptr_t l_data_ptr = left.DataPtr();
	if (!TieIsBreakable(col_idx, l_data_ptr, sorting_state.blob_layout)) {
		// Quick check to see if ties can be broken
		return 0;
	}
	// Align the pointers with the column that is being compared
	const auto &tie_col_offset = sorting_state.blob_layout.GetOffsets()[col_idx];
	l_data_ptr += tie_col_offset;
	data_ptr_t r_data_ptr = right.DataPtr() + tie_col_offset;
	// Now do the comparison
	const int order = sorting_state.order_types[tie_col] == OrderType::DESCENDING ? -1 : 1;
	const auto &type = left.layout.GetTypes()[col_idx];
	int result;
	if (external) {
		// Unswizzle to restore the pointer
		data_ptr_t l_heap_ptr = left.HeapPtr();
		data_ptr_t r_heap_ptr = right.HeapPtr();
		UnswizzleSingleValue(l_data_ptr, l_heap_ptr, type);
		UnswizzleSingleValue(r_data_ptr, r_heap_ptr, type);
		// Compare
		result = CompareVal(l_data_ptr, r_data_ptr, type, order);
		// Swizzle the pointer back
		SwizzleSingleValue(l_data_ptr, l_heap_ptr, type);
		SwizzleSingleValue(r_data_ptr, r_heap_ptr, type);
	} else {
		result = CompareVal(l_data_ptr, r_data_ptr, type, order);
	}
	return result;
}

static void SortTiedStrings(BufferManager &buffer_manager, const data_ptr_t dataptr, const idx_t &start,
                            const idx_t &end, const idx_t &tie_col, bool *ties, const data_ptr_t blob_ptr,
                            const SortingState &sorting_state) {
	const auto row_width = sorting_state.blob_layout.GetRowWidth();
	const idx_t &col_idx = sorting_state.sorting_to_blob_col.at(tie_col);
	// Locate the first blob row in question
	data_ptr_t row_ptr = dataptr + start * sorting_state.entry_size;
	data_ptr_t blob_row_ptr = blob_ptr + Load<idx_t>(row_ptr + sorting_state.comparison_size) * row_width;
	if (!TieIsBreakable(col_idx, blob_row_ptr, sorting_state.blob_layout)) {
		// Quick check to see if ties can be broken
		return;
	}
	// Fill pointer array for sorting
	auto ptr_block =
	    buffer_manager.Allocate(MaxValue((end - start) * sizeof(data_ptr_t), (idx_t)Storage::BLOCK_ALLOC_SIZE));
	auto entry_ptrs = (data_ptr_t *)ptr_block->Ptr();
	for (idx_t i = start; i < end; i++) {
		entry_ptrs[i - start] = row_ptr;
		row_ptr += sorting_state.entry_size;
	}
	// Slow pointer-based sorting
	const int order = sorting_state.order_types[tie_col] == OrderType::DESCENDING ? -1 : 1;
	const auto &tie_col_offset = sorting_state.blob_layout.GetOffsets()[col_idx];
	std::sort(entry_ptrs, entry_ptrs + end - start,
	          [&blob_ptr, &order, &sorting_state, &tie_col_offset, &row_width](const data_ptr_t l, const data_ptr_t r) {
		          // use indices to find strings in blob
		          idx_t left_idx = Load<idx_t>(l + sorting_state.comparison_size);
		          idx_t right_idx = Load<idx_t>(r + sorting_state.comparison_size);
		          string_t left_val = Load<string_t>(blob_ptr + left_idx * row_width + tie_col_offset);
		          string_t right_val = Load<string_t>(blob_ptr + right_idx * row_width + tie_col_offset);
		          return TemplatedCompareVal<string_t>(left_val, right_val, order) < 0;
	          });
	// Re-order
	auto temp_block =
	    buffer_manager.Allocate(MaxValue((end - start) * sorting_state.entry_size, (idx_t)Storage::BLOCK_ALLOC_SIZE));
	data_ptr_t temp_ptr = temp_block->Ptr();
	for (idx_t i = 0; i < end - start; i++) {
		memcpy(temp_ptr, entry_ptrs[i], sorting_state.entry_size);
		temp_ptr += sorting_state.entry_size;
	}
	memcpy(dataptr + start * sorting_state.entry_size, temp_block->Ptr(), (end - start) * sorting_state.entry_size);
	// Determine if there are still ties (if this is not the last column)
	if (tie_col < sorting_state.column_count - 1) {
		data_ptr_t idx_ptr = dataptr + start * sorting_state.entry_size + sorting_state.comparison_size;
		// Load current entry
		idx_t current_idx = Load<idx_t>(idx_ptr);
		string_t current_val = Load<string_t>(blob_ptr + current_idx * row_width + tie_col_offset);
		for (idx_t i = 0; i < end - start - 1; i++) {
			// Load next entry
			idx_ptr += sorting_state.entry_size;
			idx_t next_idx = Load<idx_t>(idx_ptr);
			string_t next_val = Load<string_t>(blob_ptr + next_idx * row_width + tie_col_offset);
			// Compare
			ties[start + i] = Equals::Operation<string_t>(current_val, next_val);
			current_val = next_val;
		}
	}
}

static void SortTiedBlobs(BufferManager &buffer_manager, SortedBlock &sb, bool *ties, data_ptr_t dataptr,
                          const idx_t &count, const idx_t &tie_col, const SortingState &sorting_state) {
	D_ASSERT(!ties[count - 1]);
	auto &blob_block = sb.sorting_data_blob->data_blocks.back();
	auto blob_handle = buffer_manager.Pin(blob_block.block);
	const data_ptr_t blob_ptr = blob_handle->Ptr();

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
		switch (sorting_state.logical_types[tie_col].InternalType()) {
		case PhysicalType::VARCHAR:
			SortTiedStrings(buffer_manager, dataptr, i, j + 1, tie_col, ties, blob_ptr, sorting_state);
			break;
		default:
			throw NotImplementedException("Cannot sort variable size column with type %s",
			                              sorting_state.logical_types[tie_col].ToString());
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
	D_ASSERT(col_offset + tie_size <= sorting_state.comparison_size);
	// align dataptr
	dataptr += col_offset;
	for (idx_t i = 0; i < count - 1; i++) {
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

static void SortInMemory(BufferManager &buffer_manager, SortedBlock &sb, const SortingState &sorting_state) {
	auto &block = sb.sorting_data_radix.back();
	const auto &count = block.count;
	auto handle = buffer_manager.Pin(block.block);
	const auto dataptr = handle->Ptr();
	// Assign an index to each row
	data_ptr_t idx_dataptr = dataptr + sorting_state.comparison_size;
	for (idx_t i = 0; i < count; i++) {
		Store<idx_t>(i, idx_dataptr);
		idx_dataptr += sorting_state.entry_size;
	}
	// All constant size sorting columns: just radix sort the whole thing
	if (sorting_state.all_constant) {
		RadixSort(buffer_manager, dataptr, count, 0, sorting_state.comparison_size, sorting_state);
		return;
	}
	// Variable size sorting columns: radix sort and break ties
	idx_t sorting_size = 0;
	idx_t col_offset = 0;
	unique_ptr<BufferHandle> ties_handle;
	bool *ties = nullptr;
	for (idx_t i = 0; i < sorting_state.column_count; i++) {
		sorting_size += sorting_state.column_sizes[i];
		if (sorting_state.constant_size[i] && i < sorting_state.column_count - 1) {
			// Add columns to the sorting size until we reach a variable size column, or the last column
			continue;
		}

		if (!ties) {
			// This is the first sort
			RadixSort(buffer_manager, dataptr, count, col_offset, sorting_size, sorting_state);
			ties_handle = buffer_manager.Allocate(MaxValue(count, (idx_t)Storage::BLOCK_ALLOC_SIZE));
			ties = (bool *)ties_handle->Ptr();
			std::fill_n(ties, count - 1, true);
			ties[count - 1] = false;
		} else {
			// For subsequent sorts, we only have to subsort the tied tuples
			SubSortTiedTuples(buffer_manager, dataptr, count, col_offset, sorting_size, ties, sorting_state);
		}

		if (sorting_state.constant_size[i] && i == sorting_state.column_count - 1) {
			// All columns are sorted, no ties to break because last column is constant size
			break;
		}

		ComputeTies(dataptr, count, col_offset, sorting_size, ties, sorting_state);
		if (!AnyTies(ties, count)) {
			// No ties, stop sorting
			break;
		}

		SortTiedBlobs(buffer_manager, sb, ties, dataptr, count, i, sorting_state);
		if (!AnyTies(ties, count)) {
			// No more ties after tie-breaking, stop
			break;
		}

		col_offset += sorting_size;
		sorting_size = 0;
	}
}

static void ReOrder(BufferManager &buffer_manager, SortedData &sd, data_ptr_t sorting_ptr, RowDataCollection &heap,
                    OrderGlobalState &gstate) {
	const idx_t &count = sd.data_blocks.back().count;
	// Pin unordered block
	auto &unordered_data_block = sd.data_blocks.back();
	auto unordered_data_handle = buffer_manager.Pin(unordered_data_block.block);
	const data_ptr_t unordered_data_ptr = unordered_data_handle->Ptr();
	// Create new block that will hold re-ordered row data
	RowDataBlock reordered_data_block(buffer_manager, unordered_data_block.capacity, unordered_data_block.entry_size);
	reordered_data_block.count = count;
	auto ordered_data_handle = buffer_manager.Pin(reordered_data_block.block);
	data_ptr_t ordered_data_ptr = ordered_data_handle->Ptr();
	// Do the actual re-ordering
	const idx_t row_width = sd.layout.GetRowWidth();
	const idx_t sorting_entry_size = gstate.sorting_state.entry_size;
	for (idx_t i = 0; i < count; i++) {
		idx_t index = Load<idx_t>(sorting_ptr);
		memcpy(ordered_data_ptr, unordered_data_ptr + index * row_width, row_width);
		ordered_data_ptr += row_width;
		sorting_ptr += sorting_entry_size;
	}
	// Deal with the heap
	if (!sd.layout.AllConstant() && gstate.external) {
		// Swizzle the pointers to offsets
		RowOperations::Swizzle(sd.layout, ordered_data_handle->Ptr(), count);
		// Create a single heap block to store the re-ordered heap
		idx_t total_byte_offset = std::accumulate(heap.blocks.begin(), heap.blocks.end(), 0,
		                                          [](idx_t a, const RowDataBlock &b) { return a + b.byte_offset; });
		idx_t heap_block_size = MaxValue(total_byte_offset, (idx_t)Storage::BLOCK_ALLOC_SIZE);
		RowDataBlock reordered_heap_block(buffer_manager, heap_block_size / 8, 8);
		reordered_heap_block.count = count;
		reordered_heap_block.byte_offset = total_byte_offset;
		auto reordered_heap_handle = buffer_manager.Pin(reordered_data_block.block);
		const data_ptr_t reordered_heap_base_ptr = reordered_heap_handle->Ptr();
		data_ptr_t reordered_heap_ptr = reordered_heap_handle->Ptr();
		// Create an ID for it
		const uint32_t heap_block_id = gstate.next_heap_block_id++;
		// Fill the heap in order
		ordered_data_ptr = ordered_data_handle->Ptr();
		const idx_t heap_ptr_offset = sd.layout.GetHeapPointerOffset();
		const idx_t heap_blockid_offset = sd.layout.GetHeapBlockIdOffset();
		const idx_t heap_offset_offset = sd.layout.GetHeapOffsetOffset();
		for (idx_t i = 0; i < count; i++) {
			// Copy the heap row
			auto heap_row_ptr = Load<data_ptr_t>(ordered_data_ptr + heap_ptr_offset);
			auto heap_row_size = Load<idx_t>(heap_row_ptr);
			memcpy(reordered_heap_ptr, heap_row_ptr, heap_row_size);
			// Store the heap blockid and heap offset over where the pointer was
			Store<uint32_t>(heap_block_id, ordered_data_ptr + heap_blockid_offset);
			Store<uint32_t>(reordered_heap_ptr - reordered_heap_base_ptr, ordered_data_ptr + heap_offset_offset);
			reordered_heap_ptr += heap_row_size;
		}
		// Keep track of the heap block ids that correspond to the row blocks
		reordered_heap_block.heap_block_id = heap_block_id;
		reordered_data_block.heap_block_ids.push_back(heap_block_id);
		// Move the re-ordered heap to the SortedData, and clear the local heap
		sd.heap_blocks.push_back(move(reordered_heap_block));
		for (auto &block : heap.blocks) {
			buffer_manager.UnregisterBlock(block.block->BlockId(), true);
		}
	} else {
		// Keep heap blocks pinned (move handles to the global state)
		D_ASSERT(heap.blocks.size() == heap.pinned_blocks.size());
		lock_guard<mutex> lock(gstate.lock);
		for (idx_t i = 0; i < heap.blocks.size(); i++) {
			gstate.pinned_blocks.push_back(move(heap.pinned_blocks[i]));
			gstate.heap_blocks.push_back(heap.blocks[i].block);
		}
	}
	// Reset the heap
	heap.pinned_blocks.clear();
	heap.blocks.clear();
	heap.count = 0;
	// Replace the unordered data block with the re-ordered data block
	buffer_manager.UnregisterBlock(unordered_data_block.block->BlockId(), true);
	sd.data_blocks.clear();
	sd.data_blocks.push_back(move(reordered_data_block));
}

//! Use the ordered sorting data to re-order the rest of the data
static void ReOrder(ClientContext &context, SortedBlock &sb, OrderLocalState &lstate, OrderGlobalState &gstate) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto sorting_handle = buffer_manager.Pin(sb.sorting_data_radix.back().block);
	const data_ptr_t sorting_ptr = sorting_handle->Ptr() + gstate.sorting_state.comparison_size;
	// Re-order variable size sorting columns
	if (!gstate.sorting_state.all_constant) {
		ReOrder(buffer_manager, *sb.sorting_data_blob, sorting_ptr, *lstate.sorting_data_heap, gstate);
	}
	// And the payload
	ReOrder(buffer_manager, *sb.payload_data, sorting_ptr, *lstate.payload_heap, gstate);
}

void PhysicalOrder::SortLocalState(ClientContext &context, OrderLocalState &lstate, OrderGlobalState &gstate) const {
	D_ASSERT(lstate.sorting_data_radix->count == lstate.payload_data->count);
	if (lstate.sorting_data_radix->count == 0) {
		return;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	const auto &sorting_state = gstate.sorting_state;
	// Move all data to a single SortedBlock
	auto sb = make_unique<SortedBlock>(buffer_manager, gstate);
	// Fixed-size sorting data
	auto sorting_block = ConcatenateBlocks(buffer_manager, *lstate.sorting_data_radix, true);
	sb->sorting_data_radix.push_back(move(sorting_block));
	// Variable-size sorting data
	if (!sorting_state.blob_layout.AllConstant()) {
		auto &blob_data = *lstate.sorting_data_blob;
		auto new_block = ConcatenateBlocks(buffer_manager, blob_data, true);
		sb->sorting_data_blob->data_blocks.push_back(move(new_block));
	}
	// Payload data
	auto payload_block = ConcatenateBlocks(buffer_manager, *lstate.payload_data, true);
	sb->payload_data->data_blocks.push_back(move(payload_block));
	// Now perform the actual sort
	SortInMemory(buffer_manager, *sb, sorting_state);
	// Re-order before the merge sort
	ReOrder(context, *sb, lstate, gstate);
	// Add the sorted block to the local state
	lstate.sorted_blocks.push_back(move(sb));
}

inline int CompareTuple(const SortedBlock &left, const SortedBlock &right, const data_ptr_t &l_ptr,
                        const data_ptr_t &r_ptr, const SortingState &sorting_state, const bool &external_sort) {
	// Compare the sorting columns one by one
	int comp_res = 0;
	data_ptr_t l_ptr_offset = l_ptr;
	data_ptr_t r_ptr_offset = r_ptr;
	for (idx_t col_idx = 0; col_idx < sorting_state.column_count; col_idx++) {
		comp_res = memcmp(l_ptr_offset, r_ptr_offset, sorting_state.column_sizes[col_idx]);
		if (comp_res == 0 && !sorting_state.constant_size[col_idx]) {
			comp_res =
			    BreakBlobTie(col_idx, *left.sorting_data_blob, *right.sorting_data_blob, sorting_state, external_sort);
		}
		if (comp_res != 0) {
			break;
		}
		l_ptr_offset += sorting_state.column_sizes[col_idx];
		r_ptr_offset += sorting_state.column_sizes[col_idx];
	}
	return comp_res;
}

class PhysicalOrderMergeTask : public Task {
public:
	PhysicalOrderMergeTask(Pipeline &parent_p, ClientContext &context_p, OrderGlobalState &state_p)
	    : parent(parent_p), context(context_p), buffer_manager(BufferManager::GetBufferManager(context_p)),
	      state(state_p), sorting_state(state_p.sorting_state) {
	}

	void Execute() override {
		ComputeWork();
		D_ASSERT(left_block->sorting_data_radix.size() == left_block->payload_data->data_blocks.size());
		D_ASSERT(right_block->sorting_data_radix.size() == right_block->payload_data->data_blocks.size());
#ifdef DEBUG
		if (!sorting_state.all_constant) {
			D_ASSERT(left_block->sorting_data_radix.size() == left_block->sorting_data_blob->data_blocks.size());
			D_ASSERT(right_block->sorting_data_radix.size() == right_block->sorting_data_blob->data_blocks.size());
		}
#endif
		// Set up the write block
		result->InitializeWrite();
		// Initialize arrays to store merge data
		bool left_smaller[PhysicalOrder::MERGE_STRIDE];
		idx_t next_entry_sizes[PhysicalOrder::MERGE_STRIDE];
		// Merge loop
		auto &left = *left_block;
		auto &right = *right_block;
		auto l_count = left.Remaining();
		auto r_count = right.Remaining();
		while (true) {
			auto l_remaining = left.Remaining();
			auto r_remaining = right.Remaining();
			if (l_remaining + r_remaining == 0) {
				// Done
				break;
			}
			const idx_t next = MinValue(l_remaining + r_remaining, PhysicalOrder::MERGE_STRIDE);
			if (l_remaining != 0 && r_remaining != 0) {
				// Compute the merge (not needed if one side is exhausted)
				ComputeMerge(next, left_smaller);
			}
			// Actually merge the data (radix, blob, and payload)
			Merge(next, left_smaller);
			if (!sorting_state.all_constant) {
				Merge(*result->sorting_data_blob, *left.sorting_data_blob, *right.sorting_data_blob, next, left_smaller,
				      next_entry_sizes);
				D_ASSERT(left.block_idx == left.sorting_data_blob->block_idx &&
				         left.entry_idx == left.sorting_data_blob->entry_idx);
				D_ASSERT(right.block_idx == right.sorting_data_blob->block_idx &&
				         right.entry_idx == right.sorting_data_blob->entry_idx);
			}
			Merge(*result->payload_data, *left.payload_data, *right.payload_data, next, left_smaller, next_entry_sizes);
			D_ASSERT(left.block_idx == left.payload_data->block_idx && left.entry_idx == left.payload_data->entry_idx);
			D_ASSERT(right.block_idx == right.payload_data->block_idx &&
			         right.entry_idx == right.payload_data->entry_idx);
		}
		D_ASSERT(result->Count() == l_count + r_count);

		lock_guard<mutex> glock(state.lock);
		parent.finished_tasks++;
		if (parent.finished_tasks == parent.total_tasks) {
			// Unregister processed data
			for (auto &sb : state.sorted_blocks) {
				sb->UnregisterSortingBlocks();
				sb->UnregisterPayloadBlocks();
			}
			state.sorted_blocks.clear();
			if (state.odd_one_out) {
				state.sorted_blocks.push_back(move(state.odd_one_out));
				state.odd_one_out = nullptr;
			}
			for (auto &sorted_block_vector : state.sorted_blocks_temp) {
				state.sorted_blocks.push_back(make_unique<SortedBlock>(buffer_manager, state));
				state.sorted_blocks.back()->AppendSortedBlocks(sorted_block_vector);
			}
			state.sorted_blocks_temp.clear();
			PhysicalOrder::ScheduleMergeTasks(parent, context, state);
		}
	}

	void ComputeWork() {
		// Acquire global lock to compute next intersection
		lock_guard<mutex> glock(state.lock);
		// Create result block
		state.sorted_blocks_temp[state.pair_idx].push_back(make_unique<SortedBlock>(buffer_manager, state));
		result = state.sorted_blocks_temp[state.pair_idx].back().get();
		// Determine which blocks must be merged
		auto &left = *state.sorted_blocks[state.pair_idx * 2];
		auto &right = *state.sorted_blocks[state.pair_idx * 2 + 1];
		const idx_t l_count = left.Count();
		const idx_t r_count = right.Count();
		// Compute the work that this thread must do using Merge Path
		idx_t l_end;
		idx_t r_end;
		if (state.l_start + state.r_start + state.block_capacity < l_count + r_count) {
			const idx_t intersection = state.l_start + state.r_start + state.block_capacity;
			ComputeIntersection(left, right, intersection, l_end, r_end);
			D_ASSERT(l_end <= l_count);
			D_ASSERT(r_end <= r_count);
			D_ASSERT(intersection == l_end + r_end);
			// Unpin after finding the intersection
			if (!sorting_state.blob_layout.AllConstant()) {
				left.sorting_data_blob->UnpinAndReset(0, 0);
				right.sorting_data_blob->UnpinAndReset(0, 0);
			}
		} else {
			l_end = l_count;
			r_end = r_count;
		}
		// Create slices of the data that this thread must merge
		left_block = left.CreateSlice(state.l_start, l_end);
		right_block = right.CreateSlice(state.r_start, r_end);
		// Update global state
		state.l_start = l_end;
		state.r_start = r_end;
		if (state.l_start == l_count && state.r_start == r_count) {
			state.pair_idx++;
			state.l_start = 0;
			state.r_start = 0;
		}
	}

	//! Compare values within SortedBlocks using a global index
	int CompareUsingGlobalIndex(SortedBlock &l, SortedBlock &r, const idx_t l_idx, const idx_t r_idx) {
		D_ASSERT(l_idx < l.Count());
		D_ASSERT(r_idx < r.Count());

		idx_t l_block_idx;
		idx_t l_entry_idx;
		l.GlobalToLocalIndex(l_idx, l_block_idx, l_entry_idx);

		idx_t r_block_idx;
		idx_t r_entry_idx;
		r.GlobalToLocalIndex(r_idx, r_block_idx, r_entry_idx);

		auto l_block_handle = buffer_manager.Pin(l.sorting_data_radix[l_block_idx].block);
		auto r_block_handle = buffer_manager.Pin(r.sorting_data_radix[r_block_idx].block);
		data_ptr_t l_ptr = l_block_handle->Ptr() + l_entry_idx * sorting_state.entry_size;
		data_ptr_t r_ptr = r_block_handle->Ptr() + r_entry_idx * sorting_state.entry_size;

		int comp_res;
		if (sorting_state.all_constant) {
			comp_res = memcmp(l_ptr, r_ptr, sorting_state.comparison_size);
		} else {
			l.sorting_data_blob->block_idx = l_block_idx;
			l.sorting_data_blob->entry_idx = l_entry_idx;
			l.sorting_data_blob->Pin();
			r.sorting_data_blob->block_idx = r_block_idx;
			r.sorting_data_blob->entry_idx = r_entry_idx;
			r.sorting_data_blob->Pin();
			comp_res = CompareTuple(l, r, l_ptr, r_ptr, sorting_state, state.external);
		}
		return comp_res;
	}

	void ComputeIntersection(SortedBlock &l, SortedBlock &r, const idx_t sum, idx_t &l_idx, idx_t &r_idx) {
		const idx_t l_count = l.Count();
		const idx_t r_count = r.Count();
		// Cover some edge cases
		if (sum >= l_count + r_count) {
			l_idx = l_count;
			r_idx = r_count;
			return;
		} else if (sum == 0) {
			l_idx = 0;
			r_idx = 0;
			return;
		} else if (l_count == 0) {
			l_idx = 0;
			r_idx = sum;
			return;
		} else if (r_count == 0) {
			r_idx = 0;
			l_idx = sum;
			return;
		}
		// Determine offsets for the binary search
		const idx_t l_offset = MinValue(l_count, sum);
		const idx_t r_offset = sum > l_count ? sum - l_count : 0;
		D_ASSERT(l_offset + r_offset == sum);
		const idx_t search_space =
		    sum > MaxValue(l_count, r_count) ? l_count + r_count - sum : MinValue(sum, MinValue(l_count, r_count));
		// Double binary search
		idx_t left = 0;
		idx_t right = search_space - 1;
		idx_t middle;
		int comp_res;
		while (left <= right) {
			middle = (left + right) / 2;
			l_idx = l_offset - middle;
			r_idx = r_offset + middle;
			if (l_idx == l_count || r_idx == 0) {
				comp_res = CompareUsingGlobalIndex(l, r, l_idx - 1, r_idx);
				if (comp_res > 0) {
					l_idx--;
					r_idx++;
				} else {
					return;
				}
				if (l_idx == 0 || r_idx == r_count) {
					return;
				} else {
					break;
				}
			}
			comp_res = CompareUsingGlobalIndex(l, r, l_idx, r_idx);
			if (comp_res > 0) {
				left = middle + 1;
			} else {
				right = middle - 1;
			}
		}
		// shift by one (if needed)
		if (l_idx == 0) {
			comp_res = CompareUsingGlobalIndex(l, r, l_idx, r_idx);
			if (comp_res > 0) {
				l_idx--;
				r_idx++;
			}
			return;
		}
		int l_r_min1 = CompareUsingGlobalIndex(l, r, l_idx, r_idx - 1);
		int l_min1_r = CompareUsingGlobalIndex(l, r, l_idx - 1, r_idx);
		if (l_r_min1 > 0 && l_min1_r < 0) {
			return;
		} else if (l_r_min1 > 0) {
			l_idx--;
			r_idx++;
		} else if (l_min1_r < 0) {
			l_idx++;
			r_idx--;
		}
	}

	//! Computes how the next 'count' tuples should be merged
	void ComputeMerge(const idx_t &count, bool *left_smaller) {
		auto &left = *left_block;
		auto &right = *right_block;
		// Store indices to restore after computing the merge
		idx_t l_block_idx = left.block_idx;
		idx_t r_block_idx = right.block_idx;
		idx_t l_entry_idx = left.entry_idx;
		idx_t r_entry_idx = right.entry_idx;
		// Data blocks and handles for both sides
		RowDataBlock *l_block;
		RowDataBlock *r_block;
		unique_ptr<BufferHandle> l_block_handle;
		unique_ptr<BufferHandle> r_block_handle;
		data_ptr_t l_ptr;
		data_ptr_t r_ptr;
		// Compute the merge of the next 'count' tuples
		idx_t compared = 0;
		while (compared < count) {
			const bool l_done = l_block_idx == left.sorting_data_radix.size();
			const bool r_done = r_block_idx == right.sorting_data_radix.size();
			if (l_done || r_done) {
				// One of the sides is exhausted, no need to compare
				break;
			}
			// Pin the radix sorting data
			if (!l_done) {
				l_block = &left.sorting_data_radix[l_block_idx];
				l_block_handle = buffer_manager.Pin(l_block->block);
				l_ptr = l_block_handle->Ptr() + l_entry_idx * sorting_state.entry_size;
			}
			if (!r_done) {
				r_block = &right.sorting_data_radix[r_block_idx];
				r_block_handle = buffer_manager.Pin(r_block->block);
				r_ptr = r_block_handle->Ptr() + r_entry_idx * sorting_state.entry_size;
			}
			const idx_t &l_count = !l_done ? l_block->count : 0;
			const idx_t &r_count = !r_done ? r_block->count : 0;
			// Compute the merge
			if (sorting_state.all_constant) {
				// All sorting columns are constant size
				for (; compared < count && l_entry_idx < l_count && r_entry_idx < r_count; compared++) {
					left_smaller[compared] = memcmp(l_ptr, r_ptr, sorting_state.comparison_size) < 0;
					const bool &l_smaller = left_smaller[compared];
					const bool r_smaller = !l_smaller;
					left_smaller[compared] = l_smaller;
					// use comparison bool (0 or 1) to increment entries and pointers
					l_entry_idx += l_smaller;
					r_entry_idx += r_smaller;
					l_ptr += l_smaller * sorting_state.entry_size;
					r_ptr += r_smaller * sorting_state.entry_size;
				}
			} else {
				// Pin the blob data
				if (!l_done) {
					left.sorting_data_blob->Pin();
				}
				if (!r_done) {
					right.sorting_data_blob->Pin();
				}
				// Merge with variable size sorting columns
				for (; compared < count && l_entry_idx < l_count && r_entry_idx < r_count; compared++) {
					left_smaller[compared] = CompareTuple(left, right, l_ptr, r_ptr, sorting_state, state.external) < 0;
					if (left_smaller[compared]) {
						l_entry_idx++;
						l_ptr += sorting_state.entry_size;
						left.sorting_data_blob->Advance();
					} else {
						r_entry_idx++;
						r_ptr += sorting_state.entry_size;
						right.sorting_data_blob->Advance();
					}
				}
			}
			// Move to the next block (if needed)
			if (!l_done && l_entry_idx == l_count) {
				l_block_idx++;
				l_entry_idx = 0;
			}
			if (!r_done && r_entry_idx == r_count) {
				r_block_idx++;
				r_entry_idx = 0;
			}
		}
		// Reset block indices before the actual merge
		// TODO: do we need to reset the heap block idx?
		if (!sorting_state.all_constant) {
			left.sorting_data_blob->UnpinAndReset(left.block_idx, left.entry_idx);
			right.sorting_data_blob->UnpinAndReset(right.block_idx, right.entry_idx);
		}
	}

	//! Merges the radix sorting blocks
	void Merge(const idx_t &count, const bool *left_smaller) {
		auto &left = *left_block;
		auto &right = *right_block;
		RowDataBlock *l_block;
		RowDataBlock *r_block;

		unique_ptr<BufferHandle> l_block_handle;
		unique_ptr<BufferHandle> r_block_handle;
		data_ptr_t l_ptr;
		data_ptr_t r_ptr;

		RowDataBlock *result_block = &result->sorting_data_radix.back();
		auto result_handle = buffer_manager.Pin(result_block->block);
		data_ptr_t result_ptr = result_handle->Ptr() + result_block->count * sorting_state.entry_size;

		idx_t copied = 0;
		while (copied < count) {
			const bool l_done = left.block_idx == left.sorting_data_radix.size();
			const bool r_done = right.block_idx == right.sorting_data_radix.size();
			// pin the blocks
			if (!l_done) {
				l_block = &left.sorting_data_radix[left.block_idx];
				l_block_handle = buffer_manager.Pin(l_block->block);
				l_ptr = l_block_handle->Ptr() + left.entry_idx * sorting_state.entry_size;
			}
			if (!r_done) {
				r_block = &right.sorting_data_radix[right.block_idx];
				r_block_handle = buffer_manager.Pin(r_block->block);
				r_ptr = r_block_handle->Ptr() + right.entry_idx * sorting_state.entry_size;
			}
			const idx_t &l_count = !l_done ? l_block->count : 0;
			const idx_t &r_count = !r_done ? r_block->count : 0;

			// create new result block (if needed)
			if (result_block->count == result_block->capacity) {
				result->CreateBlock();
				result_block = &result->sorting_data_radix.back();
				result_handle = buffer_manager.Pin(result_block->block);
				result_ptr = result_handle->Ptr();
			}
			// copy using computed merge
			if (!l_done && !r_done) {
				// neither left nor right side is exhausted
				MergeConstantSize(l_ptr, left.entry_idx, l_count, r_ptr, right.entry_idx, r_count, result_block,
				                  result_ptr, sorting_state.entry_size, left_smaller, copied, count);
			} else if (r_done) {
				// right side is exhausted
				FlushConstantSize(l_ptr, left.entry_idx, l_count, result_block, result_ptr, sorting_state.entry_size,
				                  copied, count);
			} else {
				// left side is exhausted
				FlushConstantSize(r_ptr, right.entry_idx, r_count, result_block, result_ptr, sorting_state.entry_size,
				                  copied, count);
			}
			// move to the next block (if needed)
			if (!l_done && left.entry_idx == left.sorting_data_radix[left.block_idx].count) {
				left.block_idx++;
				left.entry_idx = 0;
			}
			if (!r_done && right.entry_idx == right.sorting_data_radix[right.block_idx].count) {
				right.block_idx++;
				right.entry_idx = 0;
			}
		}
	}

	//! Merges SortedData
	void Merge(SortedData &result_data, SortedData &l_data, SortedData &r_data, const idx_t &count,
	           const bool *left_smaller, idx_t *next_entry_sizes) {
		const auto &layout = result_data.layout;
		const idx_t row_width = layout.GetRowWidth();
		const idx_t heap_blockid_offset = layout.GetHeapBlockIdOffset();
		const idx_t heap_offset_offset = layout.GetHeapOffsetOffset();

		// Left and right row data to merge
		RowDataBlock *l_data_block;
		RowDataBlock *r_data_block;
		unique_ptr<BufferHandle> l_data_block_handle;
		unique_ptr<BufferHandle> r_data_block_handle;
		data_ptr_t l_ptr;
		data_ptr_t r_ptr;
		// Accompanying left and right heap data (if needed)
		uint32_t l_heap_block_id;
		uint32_t r_heap_block_id;
		unique_ptr<BufferHandle> l_heap_handle;
		unique_ptr<BufferHandle> r_heap_handle;
		data_ptr_t l_heap_base_ptr;
		data_ptr_t r_heap_base_ptr;
		data_ptr_t l_heap_ptr;
		data_ptr_t r_heap_ptr;

		// Result rows to write to
		RowDataBlock *result_data_block = &result_data.data_blocks.back();
		auto result_data_handle = buffer_manager.Pin(result_data_block->block);
		data_ptr_t result_data_ptr = result_data_handle->Ptr() + result_data_block->count * row_width;
		// Result heap to write to
		RowDataBlock *result_heap_block;
		unique_ptr<BufferHandle> result_heap_handle;
		data_ptr_t result_heap_base_ptr;
		data_ptr_t result_heap_ptr;
		uint32_t result_heap_block_id;
		bool heap_block_full = false;
		if (!layout.AllConstant() && state.external) {
			result_heap_block = &result_data.heap_blocks.back();
			result_heap_handle = buffer_manager.Pin(result_heap_block->block);
			result_heap_base_ptr = result_heap_handle->Ptr();
			result_heap_ptr = result_heap_handle->Ptr() + result_heap_block->byte_offset;
			result_heap_block_id = result_heap_block->heap_block_id;
		}

		idx_t copied = 0;
		while (copied < count) {
			const bool l_done = l_data.block_idx == l_data.data_blocks.size();
			const bool r_done = r_data.block_idx == r_data.data_blocks.size();
			// Pin the row data blocks
			if (!l_done) {
				l_data_block = &l_data.data_blocks[l_data.block_idx];
				l_data_block_handle = buffer_manager.Pin(l_data_block->block);
				l_ptr = l_data_block_handle->Ptr() + l_data.entry_idx * row_width;
			}
			if (!r_done) {
				r_data_block = &r_data.data_blocks[r_data.block_idx];
				r_data_block_handle = buffer_manager.Pin(r_data_block->block);
				r_ptr = r_data_block_handle->Ptr() + r_data.entry_idx * row_width;
			}
			const idx_t &l_count = !l_done ? l_data_block->count : 0;
			const idx_t &r_count = !r_done ? r_data_block->count : 0;
			// Create new result data block (if needed)
			if (result_data_block->count == result_data_block->capacity) {
				result_data.CreateDataBlock();
				result_data_block = &result_data.data_blocks.back();
				result_data_handle = buffer_manager.Pin(result_data_block->block);
				result_data_ptr = result_data_handle->Ptr();
				// Add heap block ID to this block (if not already there)
				if (!layout.AllConstant() && state.external &&
				    result_data_block->heap_block_ids.back() != result_heap_block_id) {
					result_data_block->heap_block_ids.push_back(result_heap_block_id);
				}
			}
			// Perform one of two ways to merge
			if (layout.AllConstant() || !state.external) {
				// If all constant size, or if we are doing an in-memory sort, we do not need to touch the heap
				if (!l_done && !r_done) {
					// Neither left nor right side is exhausted
					MergeConstantSize(l_ptr, l_data.entry_idx, l_count, r_ptr, r_data.entry_idx, r_count,
					                  result_data_block, result_data_ptr, row_width, left_smaller, copied, count);
				} else if (r_done) {
					// Right side is exhausted
					FlushConstantSize(l_ptr, l_data.entry_idx, l_count, result_data_block, result_data_ptr, row_width,
					                  copied, count);
				} else {
					// Left side is exhausted
					FlushConstantSize(r_ptr, r_data.entry_idx, r_count, result_data_block, result_data_ptr, row_width,
					                  copied, count);
				}
			} else {
				// External sorting with variable size data. Pin the heap blocks too
				if (!l_done) {
					l_heap_block_id = Load<uint32_t>(l_ptr + layout.GetHeapBlockIdOffset());
					l_heap_handle = buffer_manager.Pin(l_data.heap_blocks[l_data.heap_block_idx].block);
					l_heap_base_ptr = l_heap_handle->Ptr();
					l_heap_ptr = l_heap_base_ptr + Load<uint32_t>(l_ptr + heap_offset_offset);
				}
				if (!r_done) {
					r_heap_block_id = Load<uint32_t>(r_ptr + layout.GetHeapBlockIdOffset());
					l_heap_handle = buffer_manager.Pin(r_data.heap_blocks[r_data.heap_block_idx].block);
					r_heap_base_ptr = r_heap_handle->Ptr();
					r_heap_ptr = r_heap_base_ptr + Load<uint32_t>(r_ptr + heap_offset_offset);
				}
				// Create new result heap block (if needed)
				if (heap_block_full) {
					result_data.CreateHeapBlock();
					result_heap_block = &result_data.heap_blocks.back();
					result_heap_handle = buffer_manager.Pin(result_heap_block->block);
					result_heap_base_ptr = result_heap_handle->Ptr();
					result_heap_ptr = result_heap_handle->Ptr() + result_heap_block->byte_offset;
					result_heap_block_id = result_heap_block->heap_block_id;
					result_data_block->heap_block_ids.push_back(result_heap_block_id);
					heap_block_full = false;
				}
				// Merge both the row data and the heap at the same time
				if (!l_done && !r_done) {
					// Both sides have data - merge
					idx_t next;
					const idx_t result_heap_capacity = result_data_block->capacity * result_data_block->entry_size;
					for (next = 0; copied + next < count && l_data.entry_idx < l_count && r_data.entry_idx < r_count;
					     next++) {
						if (l_heap_block_id != Load<uint32_t>(l_ptr + heap_blockid_offset)) {
							// Next left row has a different heap block
							l_data.heap_block_idx++;
							break;
						}
						if (r_heap_block_id != Load<uint32_t>(r_ptr + heap_blockid_offset)) {
							// Next right row has a different heap block
							r_data.heap_block_idx++;
							break;
						}
						// Get the entry size of next entry that will be copied, and check if it will fit
						const bool &l_smaller = left_smaller[copied + next];
						idx_t l_size = Load<idx_t>(l_heap_ptr);
						idx_t r_size = Load<idx_t>(r_heap_ptr);
						idx_t entry_size = l_smaller * l_size + !l_smaller * r_size;
						if (result_data_block->byte_offset + entry_size > result_heap_capacity) {
							heap_block_full = true;
							break;
						}
						// Copy fixed size row, update heap block ID and offset, and copy heap row (from the smaller
						// side) Then update the corresponding indices and pointers from that side
						if (l_smaller) {
							memcpy(result_data_ptr, l_ptr, row_width);
							Store<uint32_t>(result_heap_block_id, result_data_ptr + heap_blockid_offset);
							Store<uint32_t>(result_heap_ptr - result_heap_base_ptr,
							                result_data_ptr + heap_offset_offset);
							memcpy(result_heap_ptr, l_heap_ptr, l_size);
							l_data.entry_idx++;
							l_ptr += row_width;
							l_heap_ptr = l_heap_base_ptr + Load<uint32_t>(l_ptr + heap_offset_offset);
						} else {
							memcpy(result_data_ptr, r_ptr, row_width);
							Store<uint32_t>(result_heap_block_id, result_data_ptr + heap_blockid_offset);
							Store<uint32_t>(result_heap_ptr - result_heap_base_ptr,
							                result_data_ptr + heap_offset_offset);
							memcpy(result_heap_ptr, r_heap_ptr, entry_size);
							r_data.entry_idx++;
							r_ptr += row_width;
							r_heap_ptr = r_heap_base_ptr + Load<uint32_t>(r_ptr + heap_offset_offset);
						}
						// Update result indices and pointers
						result_data_block->count++;
						result_data_block->byte_offset += entry_size;
						result_data_ptr += row_width;
						result_heap_ptr += entry_size;
					}
					copied += next;
				} else if (r_done) {
					// Right side is exhausted - flush left
					FlushVariableSize(layout, l_count, l_ptr, l_data.entry_idx, l_heap_ptr, l_heap_block_id,
					                  result_data_block, result_data_ptr, result_heap_block, result_heap_ptr,
					                  result_heap_base_ptr, copied, count, heap_block_full);
				} else {
					// Left side is exhausted - flush right
					FlushVariableSize(layout, r_count, r_ptr, r_data.entry_idx, r_heap_ptr, r_heap_block_id,
					                  result_data_block, result_data_ptr, result_heap_block, result_heap_ptr,
					                  result_heap_base_ptr, copied, count, heap_block_full);
				}
			}
			// move to new data blocks (if needed)
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

	void FlushVariableSize(const RowLayout &layout, const idx_t &source_count, data_ptr_t &source_data_ptr,
	                       idx_t &source_entry_idx, const data_ptr_t &source_heap_base_ptr,
	                       uint32_t source_heap_block_id, RowDataBlock *target_data_block, data_ptr_t &target_data_ptr,
	                       RowDataBlock *target_heap_block, data_ptr_t &target_heap_ptr,
	                       const data_ptr_t &target_heap_base_ptr, idx_t &copied, const idx_t &count,
	                       bool &target_block_full) {
		const idx_t row_width = layout.GetRowWidth();
		const idx_t heap_blockid_offset = layout.GetHeapBlockIdOffset();
		const idx_t heap_offset_offset = layout.GetHeapOffsetOffset();
		const uint32_t target_heap_block_id = target_heap_block->heap_block_id;
		// Flush source into target
		idx_t next;
		data_ptr_t source_heap_ptr;
		const idx_t &target_heap_capacity = target_heap_block->capacity * target_heap_block->entry_size;
		for (next = 0; copied + next < count && source_entry_idx < source_count; next++) {
			if (source_heap_block_id != Load<uint32_t>(source_data_ptr + heap_blockid_offset)) {
				// Next source row has a different heap block
				break;
			}
			source_heap_ptr = source_heap_base_ptr + Load<uint32_t>(source_data_ptr + heap_offset_offset);
			idx_t entry_size = Load<idx_t>(source_heap_ptr);
			if (target_heap_block->byte_offset + entry_size > target_heap_capacity) {
				target_block_full = true;
				break;
			}
			// Copy fixed size row, update heap block ID and offset, and copy heap row (from the smaller side)
			// Then update the corresponding indices and pointers from that side
			memcpy(target_data_ptr, source_data_ptr, row_width);
			Store<uint32_t>(target_heap_block_id, target_data_ptr + heap_blockid_offset);
			Store<uint32_t>(target_heap_ptr - target_heap_base_ptr, target_data_ptr + heap_offset_offset);
			memcpy(target_heap_ptr, source_heap_ptr, entry_size);
			source_entry_idx++;
			source_data_ptr += row_width;
			source_heap_ptr = source_heap_base_ptr + Load<uint32_t>(source_data_ptr + heap_offset_offset);
			// Update result indices and pointers
			target_data_block->count++;
			target_data_block->byte_offset += entry_size;
			target_data_ptr += row_width;
			target_heap_ptr += entry_size;
		}
		copied += next;
	}

private:
	Pipeline &parent;
	ClientContext &context;
	BufferManager &buffer_manager;
	OrderGlobalState &state;
	const SortingState &sorting_state;

	unique_ptr<SortedBlock> left_block;
	unique_ptr<SortedBlock> right_block;
	SortedBlock *result;
};

bool PhysicalOrder::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state_p) {
	this->sink_state = move(state_p);
	auto &state = (OrderGlobalState &)*this->sink_state;
	if (state.sorted_blocks.empty()) {
		return true;
	}
	// Set total count
	for (auto &sb : state.sorted_blocks) {
		state.total_count += sb->sorting_data_radix.back().count;
	}
	// Use the data that we have to determine which block size to use during the merge
	const auto &sorting_state = state.sorting_state;
	for (auto &sb : state.sorted_blocks) {
		auto &block = sb->sorting_data_radix.back();
		state.block_capacity = MaxValue(state.block_capacity, block.capacity);
	}
	// Sorting heap data
	if (!sorting_state.all_constant && state.external) {
		for (auto &sb : state.sorted_blocks) {
			auto &heap_block = sb->sorting_data_blob->heap_blocks.back();
			state.sorting_heap_capacity =
			    MaxValue(state.sorting_heap_capacity, heap_block.capacity * heap_block.entry_size);
		}
	}
	// Payload heap data
	const auto &payload_layout = state.payload_layout;
	if (!payload_layout.AllConstant() && state.external) {
		for (auto &sb : state.sorted_blocks) {
			auto &heap_block = sb->payload_data->heap_blocks.back();
			state.payload_heap_capacity =
			    MaxValue(state.sorting_heap_capacity, heap_block.capacity * heap_block.entry_size);
		}
	}
	// Start the merge or finish if a merge is not necessary
	if (state.sorted_blocks.size() > 1) {
		// More than one block - merge
		PhysicalOrder::ScheduleMergeTasks(pipeline, context, state);
		return false;
	} else {
		// Clean up sorting data - payload is sorted
		for (auto &sb : state.sorted_blocks) {
			sb->UnregisterSortingBlocks();
		}
		return true;
	}
}

void PhysicalOrder::ScheduleMergeTasks(Pipeline &pipeline, ClientContext &context, OrderGlobalState &state) {
	D_ASSERT(state.sorted_blocks_temp.empty());
	if (state.sorted_blocks.size() == 1) {
		for (auto &sb : state.sorted_blocks) {
			sb->UnregisterSortingBlocks();
		}
		pipeline.Finish();
		return;
	}
	// Uneven amount of blocks - keep one on the side
	auto num_blocks = state.sorted_blocks.size();
	if (num_blocks % 2 == 1) {
		state.odd_one_out = move(state.sorted_blocks.back());
		state.sorted_blocks.pop_back();
		num_blocks--;
	}
	// Init merge path path indices
	state.pair_idx = 0;
	state.l_start = 0;
	state.r_start = 0;
	// Compute how many tasks there will be
	idx_t num_tasks = 0;
	const idx_t tuples_per_block = state.block_capacity;
	for (idx_t block_idx = 0; block_idx < num_blocks; block_idx += 2) {
		auto &left = *state.sorted_blocks[block_idx];
		auto &right = *state.sorted_blocks[block_idx + 1];
		const idx_t count = left.Count() + right.Count();
		num_tasks += (count + tuples_per_block - 1) / tuples_per_block;
		// allocate room for merge results
		state.sorted_blocks_temp.emplace_back();
	}

	// schedule the tasks
	pipeline.total_tasks += num_tasks;
	for (idx_t tnum = 0; tnum < num_tasks; tnum++) {
		auto new_task = make_unique<PhysicalOrderMergeTask>(pipeline, context, state);
		TaskScheduler::GetScheduler(context).ScheduleTask(pipeline.token, move(new_task));
	}
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalOrderOperatorState : public PhysicalOperatorState {
public:
	PhysicalOrderOperatorState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), initialized(false), global_entry_idx(0), block_idx(0), entry_idx(0) {
	}
	bool initialized;

	SortedData *payload_data;
	Vector addresses = Vector(LogicalType::POINTER);

	idx_t global_entry_idx;
	idx_t block_idx;
	idx_t entry_idx;
};

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(*this, children[0].get());
}

static void Scan(ClientContext &context, DataChunk &chunk, PhysicalOrderOperatorState &state, OrderGlobalState &gstate,
                 const idx_t &scan_count) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto &payload_data = *state.payload_data;
	const auto &layout = gstate.payload_layout;
	const idx_t &row_width = layout.GetRowWidth();
	vector<unique_ptr<BufferHandle>> handles;
	// Set up a batch of pointers to scan data from
	idx_t count = 0;
	auto data_pointers = FlatVector::GetData<data_ptr_t>(state.addresses);
	while (count < scan_count) {
		// Grab the row data block
		auto &data_block = payload_data.data_blocks[state.block_idx];
		auto data_handle = buffer_manager.Pin(data_block.block);
		data_ptr_t payl_dataptr = data_handle->Ptr() + state.entry_idx * layout.GetRowWidth();
		// Check how many rows we can read from the same heap block
		idx_t next = MinValue(data_block.count - state.entry_idx, scan_count - count);
		if (!layout.AllConstant() && gstate.external) {
			const idx_t heap_block_id_offset = layout.GetHeapBlockIdOffset();
			const uint32_t heap_block_id = Load<uint32_t>(payl_dataptr + heap_block_id_offset);
			for (idx_t i = 0; i < next; i++) {
				if (heap_block_id != Load<uint32_t>(payl_dataptr + heap_block_id_offset)) {
					next = i;
					break;
				}
				payl_dataptr += row_width;
			}
			// Unswizzle the offsets back to pointers
			auto heap_handle =
			    buffer_manager.Pin(payload_data.heap_blocks[payload_data.GetHeapBlockIndex(heap_block_id)].block);
			RowOperations::Unswizzle(layout, data_handle->Ptr() + state.entry_idx * layout.GetRowWidth(),
			                         heap_handle->Ptr(), next);
			handles.push_back(move(heap_handle));
		}
		// Set up the next pointers
		payl_dataptr = data_handle->Ptr() + state.entry_idx * layout.GetRowWidth();
		handles.push_back(move(data_handle));
		for (idx_t i = 0; i < next; i++) {
			data_pointers[count + i] = payl_dataptr;
			payl_dataptr += layout.GetRowWidth();
		}
		// Update state indices
		state.entry_idx += next;
		if (state.entry_idx == data_block.count) {
			state.block_idx++;
			state.entry_idx = 0;
		}
		count += next;
	}
	D_ASSERT(count == scan_count);
	state.global_entry_idx += scan_count;
	// Deserialize the payload data
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		const auto col_offset = layout.GetOffsets()[col_idx];
		RowOperations::Gather(state.addresses, FlatVector::INCREMENTAL_SELECTION_VECTOR, chunk.data[col_idx],
		                      FlatVector::INCREMENTAL_SELECTION_VECTOR, scan_count, col_offset, col_idx);
	}
	chunk.SetCardinality(scan_count);
}

void PhysicalOrder::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                     PhysicalOperatorState *state_p) const {
	auto &state = *reinterpret_cast<PhysicalOrderOperatorState *>(state_p);
	auto &gstate = (OrderGlobalState &)*this->sink_state;

	if (gstate.sorted_blocks.empty()) {
		return;
	}

	if (!state.initialized) {
		D_ASSERT(gstate.sorted_blocks.back()->Count() == gstate.total_count);
		state.payload_data = gstate.sorted_blocks.back()->payload_data.get();
		state.initialized = true;
	}

	auto next = MinValue((idx_t)STANDARD_VECTOR_SIZE, gstate.total_count - state.global_entry_idx);
	Scan(context.client, chunk, state, gstate, next);
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
