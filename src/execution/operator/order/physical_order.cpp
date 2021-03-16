#include "duckdb/execution/operator/order/physical_order.hpp"

#include "blockquicksort_wrapper.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
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

	const vector<OrderType> ORDER_TYPES;
	const vector<OrderByNullType> ORDER_BY_NULL_TYPES;
	const vector<LogicalType> TYPES;
	const vector<BaseStatistics *> STATS;

	const vector<bool> HAS_NULL;
	const vector<bool> CONSTANT_SIZE;
	const vector<idx_t> COL_SIZE;
};

struct PayloadState {
	const bool HAS_VARIABLE_SIZE;
	const idx_t VALIDITYMASK_SIZE;
	const idx_t ENTRY_SIZE;
};

class OrderGlobalState : public GlobalOperatorState {
public:
	explicit OrderGlobalState(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
	}
	//! The lock for updating the order global state
	mutex lock;
	//! The buffer manager
	BufferManager &buffer_manager;

	//! Sorting columns, and variable size sorting data (if any)
	unique_ptr<RowChunk> sorting_block;
	vector<unique_ptr<RowChunk>> var_sorting_blocks;
	vector<unique_ptr<RowChunk>> var_sorting_sizes;

	//! Payload data (and payload entry sizes if there is variable size data)
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

	//! Sorting columns, and variable size sorting data (if any)
	unique_ptr<RowChunk> sorting_block = nullptr;
	vector<unique_ptr<RowChunk>> var_sorting_blocks;
	vector<unique_ptr<RowChunk>> var_sorting_sizes;

	//! Payload data (and payload entry sizes if there is variable size data)
	unique_ptr<RowChunk> payload_block = nullptr;
	unique_ptr<RowChunk> sizes_block = nullptr;

	//! Constant buffers allocated for vector serialization
	const SelectionVector *sel_ptr = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	data_ptr_t key_locations[STANDARD_VECTOR_SIZE];
	data_ptr_t validitymask_locations[STANDARD_VECTOR_SIZE];
	idx_t entry_sizes[STANDARD_VECTOR_SIZE];
};

template <class T>
static idx_t TemplatedGetSize(Value min, Value max) {
	T min_val = min.GetValue<T>();
	T max_val = max.GetValue<T>();
	idx_t size = sizeof(T);
	T max_in_size = (1 << ((size - 1) * 8 - 1)) - 1;
	while (max_val < max_in_size && min_val > -max_in_size) {
		size--;
		max_in_size = (1 << ((size - 1) * 8 - 1)) - 1;
	}
	return size;
}

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

		// compute column sizes
		auto physical_type = expr.return_type.InternalType();
		constant_size.push_back(TypeIsConstantSize(physical_type));
		idx_t col_size = GetTypeIdSize(expr.return_type.InternalType());
		if (expr.stats) {
			// TODO: test this statistics thing
			if (expr.return_type.IsNumeric()) {
				auto num_stats = (NumericStatistics &)*expr.stats;
				switch (physical_type) {
				case PhysicalType::INT16:
					col_size = TemplatedGetSize<int16_t>(num_stats.min, num_stats.max);
					break;
				case PhysicalType::INT32:
					col_size = TemplatedGetSize<int32_t>(num_stats.min, num_stats.max);
					break;
				case PhysicalType::INT64:
					col_size = TemplatedGetSize<int64_t>(num_stats.min, num_stats.max);
					break;
				case PhysicalType::INT128:
					col_size = TemplatedGetSize<hugeint_t>(num_stats.min, num_stats.max);
					break;
				default:
					// have to use full size for floating point numbers
					break;
				}
			} else if (expr.return_type == LogicalType::VARCHAR) {
				auto str_stats = (StringStatistics &)*expr.stats;
				col_size = MinValue(str_stats.max_string_length, StringStatistics::MAX_STRING_MINMAX_SIZE);
			}
			// null handling
            has_null.push_back(expr.stats->has_null);
		} else {
			if (!TypeIsConstantSize(physical_type)) {
				switch (physical_type) {
				case PhysicalType::VARCHAR:
					col_size = StringStatistics::MAX_STRING_MINMAX_SIZE;
				default:
					// do nothing
					break;
				}
			}
            has_null.push_back(true);
		}

		// increment entry size with the column size
		if (has_null.back()) {
			col_size++;
		}
		entry_size += col_size;
		col_sizes.push_back(col_size);

		// create RowChunks for variable size sorting columns in order to resolve
		if (TypeIsConstantSize(physical_type)) {
			state->var_sorting_blocks.push_back(nullptr);
			state->var_sorting_sizes.push_back(nullptr);
		} else {
			// besides the prefix, variable size sorting columns are also fully serialized, along with offsets
			state->var_sorting_blocks.push_back(
			    make_unique<RowChunk>(buffer_manager, Storage::BLOCK_ALLOC_SIZE / 8, 8));
			state->var_sorting_sizes.push_back(make_unique<RowChunk>(
			    buffer_manager, (idx_t)Storage::BLOCK_ALLOC_SIZE / sizeof(idx_t) + 1, sizeof(idx_t)));
		}
	}
	// make room for an 'index' column at the end
	entry_size += sizeof(idx_t);

	state->sorting_state = unique_ptr<SortingState>(new SortingState {
	    entry_size, order_types, order_by_null_types, types, stats, has_null, constant_size, col_sizes});
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

static void ComputeEntrySizes(Vector &v, idx_t count, idx_t entry_sizes[]) {
	VectorData vdata;
	v.Orrify(count, vdata);
	switch (v.GetType().InternalType()) {
	case PhysicalType::VARCHAR: {
		auto strings = (string_t *)vdata.data;
		for (idx_t i = 0; i < count; i++) {
			entry_sizes[i] += strings[vdata.sel->get_index(i)].GetSize();
		}
		break;
	}
	default:
		throw NotImplementedException("Variable size type not implemented for sorting!");
	}
}

static void ComputeEntrySizes(DataChunk &input, idx_t entry_sizes[], idx_t entry_size) {
	// fill array with constant portion of payload entry size
	std::fill_n(entry_sizes, input.size(), entry_size);

	// compute size of the constant portion of the payload columns
	VectorData vdata;
	for (idx_t col_idx = 0; col_idx < input.data.size(); col_idx++) {
		auto physical_type = input.data[col_idx].GetType().InternalType();
		if (TypeIsConstantSize(physical_type)) {
			continue;
		}
		ComputeEntrySizes(input.data[col_idx], input.size(), entry_sizes);
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
		for (idx_t i = 0; i < gstate.var_sorting_blocks.size(); i++) {
			if (gstate.var_sorting_blocks[i]) {
				lstate.var_sorting_blocks.push_back(make_unique<RowChunk>(*gstate.var_sorting_blocks[i]));
				lstate.var_sorting_sizes.push_back(make_unique<RowChunk>(*gstate.var_sorting_sizes[i]));
			} else {
				lstate.var_sorting_blocks.push_back(nullptr);
				lstate.var_sorting_sizes.push_back(nullptr);
			}
		}
	}

	// obtain sorting columns
	auto &sort = lstate.sort;
	gstate.executor.Execute(input, sort);

	// build and serialize sorting data
	lstate.sorting_block->Build(sort.size(), lstate.key_locations, nullptr);
	for (idx_t sort_col = 0; sort_col < sort.data.size(); sort_col++) {
		bool has_null = sorting_state.HAS_NULL[sort_col];
		bool nulls_first = sorting_state.ORDER_BY_NULL_TYPES[sort_col] == OrderByNullType::NULLS_FIRST;
		bool desc = sorting_state.ORDER_TYPES[sort_col] == OrderType::DESCENDING;
		idx_t size_in_bytes = StringStatistics::MAX_STRING_MINMAX_SIZE; // TODO: compute this
		lstate.sorting_block->SerializeVectorSortable(sort.data[sort_col], sort.size(), *lstate.sel_ptr, sort.size(),
		                                              lstate.key_locations, desc, has_null, nulls_first, size_in_bytes);
	}

	// also fully serialize variable size sorting columns
	for (idx_t sort_col = 0; sort_col < sort.data.size(); sort_col++) {
		if (TypeIsConstantSize(sort.data[sort_col].GetType().InternalType())) {
			continue;
		}
		auto &var_sizes = *lstate.var_sorting_sizes[sort_col];
		auto &var_block = *lstate.var_sorting_blocks[sort_col];
		// compute entry sizes
		switch (sort.data[sort_col].GetType().InternalType()) {
		case PhysicalType::VARCHAR:
			std::fill_n(lstate.entry_sizes, input.size(), (idx_t)string_t::PREFIX_LENGTH);
			break;
		default:
			throw NotImplementedException("Sorting variable length type");
		}
		ComputeEntrySizes(sort.data[sort_col], sort.size(), lstate.entry_sizes);
		// build and serialize entry sizes
		var_sizes.Build(sort.size(), lstate.key_locations, nullptr);
		for (idx_t i = 0; i < input.size(); i++) {
			Store<idx_t>(lstate.entry_sizes[i], lstate.key_locations[i]);
		}
		// build and serialize variable size entries
		var_block.Build(sort.size(), lstate.key_locations, lstate.entry_sizes);
		var_block.SerializeVector(sort.data[sort_col], sort.size(), *lstate.sel_ptr, input.size(), 0,
		                          lstate.key_locations, lstate.validitymask_locations);
	}

	// compute entry sizes of payload columns if there are variable size columns
	if (payload_state.HAS_VARIABLE_SIZE) {
		ComputeEntrySizes(input, lstate.entry_sizes, payload_state.ENTRY_SIZE);
		lstate.sizes_block->Build(input.size(), lstate.key_locations, nullptr);
		for (idx_t i = 0; i < input.size(); i++) {
			Store<idx_t>(lstate.entry_sizes[i], lstate.key_locations[i]);
		}
		gstate.payload_block->Build(input.size(), lstate.key_locations, lstate.entry_sizes);
	} else {
		gstate.payload_block->Build(input.size(), lstate.key_locations, nullptr);
	}

	// serialize payload data
	for (idx_t i = 0; i < input.size(); i++) {
		memset(lstate.key_locations[i], -1, payload_state.VALIDITYMASK_SIZE);
		lstate.validitymask_locations[i] = lstate.key_locations[i];
		lstate.key_locations[i] += payload_state.VALIDITYMASK_SIZE;
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
	for (idx_t i = 0; i < lstate.var_sorting_blocks.size(); i++) {
		if (!lstate.var_sorting_blocks[i]) {
			continue;
		}
		for (idx_t b = 0; b < lstate.var_sorting_blocks[i]->blocks.size(); b++) {
			gstate.var_sorting_blocks[i]->count += lstate.var_sorting_blocks[i]->count;
			gstate.var_sorting_sizes[i]->count += lstate.var_sorting_sizes[i]->count;
			gstate.var_sorting_blocks[i]->blocks.push_back(move(lstate.var_sorting_blocks[i]->blocks[b]));
			gstate.var_sorting_sizes[i]->blocks.push_back(move(lstate.var_sorting_sizes[i]->blocks[b]));
		}
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

static void ComputeTies(data_ptr_t dataptr, const idx_t &start, const idx_t &end, const idx_t &tie_col, bool ties[],
                        const SortingState &sorting_state) {
	idx_t tie_size = 0;
	for (idx_t col_idx = 0; col_idx <= tie_col; col_idx++) {
		tie_size += sorting_state.COL_SIZE[col_idx];
	}
	for (idx_t i = start; i < end - 1; i++) {
		ties[i] = memcmp(dataptr, dataptr + sorting_state.ENTRY_SIZE, tie_size) == 0;
		dataptr += sorting_state.ENTRY_SIZE;
	}
	ties[end - 1] = false;
}

static void BreakStringTies(const data_ptr_t dataptr, const idx_t &start, const idx_t &end, const idx_t &tie_col,
                            bool ties[], data_ptr_t var_dataptr, data_ptr_t sizes_ptr,
                            const SortingState &sorting_state) {
	auto entry_ptrs = unique_ptr<data_ptr_t[]>(new data_ptr_t[end - start]);
	for (idx_t i = start; i < end; i++) {
		entry_ptrs[i - start] = dataptr + i * sorting_state.ENTRY_SIZE;
	}

	// slow pointer-based sorting
	const int order = sorting_state.ORDER_TYPES[tie_col] == OrderType::DESCENDING ? -1 : 1;
	idx_t *sizes = (idx_t *)sizes_ptr;
	BlockQuickSort::Sort(entry_ptrs.get(), entry_ptrs.get() + (end - start),
	                     [&var_dataptr, &sizes, &order, &sorting_state](const data_ptr_t l, const data_ptr_t r) {
		                     idx_t left_idx = Load<idx_t>(l + sorting_state.ENTRY_SIZE - sizeof(idx_t));
		                     idx_t right_idx = Load<idx_t>(r + sorting_state.ENTRY_SIZE - sizeof(idx_t));
		                     data_ptr_t left = var_dataptr + sizes[left_idx];
		                     data_ptr_t right = var_dataptr + sizes[right_idx];
		                     return order * strncmp((const char *)left + sizeof(idx_t),
		                                            (const char *)right + sizeof(idx_t),
		                                            MinValue(Load<idx_t>(left), Load<idx_t>(right)));
	                     });

	// re-order
	auto temp = unique_ptr<data_t[]>(new data_t[(end - start) * sorting_state.ENTRY_SIZE]);
	data_ptr_t temp_ptr = temp.get();
	for (idx_t i = 0; i < end - start; i++) {
		memcpy(temp_ptr, entry_ptrs[i], sorting_state.ENTRY_SIZE);
		temp_ptr += sorting_state.ENTRY_SIZE;
	}
	memcpy(dataptr + start * sorting_state.ENTRY_SIZE, temp.get(), (end - start) * sorting_state.ENTRY_SIZE);

	// determine if there are still ties (if needed)
	if (tie_col < sorting_state.ORDER_TYPES.size() - 1) {
		idx_t current_idx = Load<idx_t>(dataptr + (start + sorting_state.ENTRY_SIZE) - sizeof(idx_t));
		idx_t current_size = Load<idx_t>(var_dataptr + sizes[current_idx]);
		idx_t next_idx, next_size;
		for (idx_t i = start; i < end - 1; i++) {
			next_idx = Load<idx_t>(dataptr + (i + sorting_state.ENTRY_SIZE) - sizeof(idx_t));
			next_size = Load<idx_t>(var_dataptr + sizes[next_idx]);
			if (current_size == next_size) {
				ties[i] = memcmp(var_dataptr + sizes[current_idx] + sizeof(idx_t),
				                 var_dataptr + sizes[next_idx] + sizeof(idx_t), current_size) == 0;
			} else {
				ties[i] = false;
			}
			current_idx = next_idx;
			current_size = next_size;
		}
	}
}

static void BreakTies(BufferManager &buffer_manager, OrderGlobalState &global_state, bool ties[], data_ptr_t dataptr,
                      const idx_t &count, const idx_t &tie_col, const SortingState &sorting_state) {
	bool any_ties = false;
	for (idx_t i = 0; i < count; i++) {
        any_ties = any_ties || ties[i];
	}
	if (!any_ties) {
		return;
	}

	auto var_block_handle = buffer_manager.Pin(global_state.var_sorting_blocks[tie_col]->blocks[0].block);
	auto var_sizes_handle = buffer_manager.Pin(global_state.var_sorting_sizes[tie_col]->blocks[0].block);
	const data_ptr_t var_dataptr = var_block_handle->node->buffer;
	const data_ptr_t sizes_ptr = var_sizes_handle->node->buffer;

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
			BreakStringTies(dataptr, i, j + 1, tie_col, ties, var_dataptr, sizes_ptr, sorting_state);
			break;
		default:
			throw NotImplementedException("Sorting of this variable size type");
		}
		i = j;
	}
}

static void SortInMemory(Pipeline &pipeline, ClientContext &context, OrderGlobalState &state) {
	const auto &sorting_state = *state.sorting_state;
	auto &buffer_manager = BufferManager::GetBufferManager(context);

	auto &block = state.sorting_block->blocks.back();
	auto handle = buffer_manager.Pin(block.block);
	auto dataptr = handle->node->buffer;

	// assign an index to each row
	idx_t sorting_size = sorting_state.ENTRY_SIZE - sizeof(idx_t);
	data_ptr_t idx_dataptr = dataptr + sorting_size;
	for (idx_t i = 0; i < block.count; i++) {
		Store<idx_t>(i, idx_dataptr);
		idx_dataptr += sorting_state.ENTRY_SIZE;
	}

	const idx_t num_cols = sorting_state.CONSTANT_SIZE.size();
	if (num_cols == 1 || std::all_of(sorting_state.CONSTANT_SIZE.begin(), sorting_state.CONSTANT_SIZE.end() - 1,
	                                 [](bool x) { return x; })) {
		// if the first n - 1 columns are all constant size, we can radix sort on all columns
		RadixSort(buffer_manager, dataptr, block.count, 0, sorting_size, sorting_state);
		if (!sorting_state.CONSTANT_SIZE[num_cols - 1]) {
			// if the final column is not constant size, we have to tie-break
			auto ties = unique_ptr<bool[]>(new bool[block.count]);
			ComputeTies(dataptr, 0, block.count, num_cols - 1, ties.get(), sorting_state);
			BreakTies(buffer_manager, state, ties.get(), dataptr, block.count, num_cols - 1, sorting_state);
		}
	} else {
		idx_t col_offset = 0;
		sorting_size = 0;
		for (idx_t i = 0; i < sorting_state.CONSTANT_SIZE.size(); i++) {
			sorting_size += sorting_state.COL_SIZE[i];
			if (!sorting_state.CONSTANT_SIZE[i]) {
				// TODO: sub-sort here if needed
				RadixSort(buffer_manager, dataptr, block.count, col_offset, sorting_size, sorting_state);
				// TODO: tie-break, and mark where we need to sub-sort the rest (if we are not on the last column
				// already)
				col_offset = sorting_size;
				sorting_size = 0;
			}
		}
	}
}

void ConcatenateBlocks(BufferManager &buffer_manager, RowChunk &row_chunk, idx_t capacity, bool variable_entry_size) {
	RowDataBlock new_block(buffer_manager, capacity, row_chunk.entry_size);
	new_block.count = row_chunk.count;
	auto new_block_handle = buffer_manager.Pin(new_block.block);
	data_ptr_t new_block_ptr = new_block_handle->node->buffer;
	for (auto &block : row_chunk.blocks) {
		auto block_handle = buffer_manager.Pin(block.block);
		if (variable_entry_size) {
			memcpy(new_block_ptr, block_handle->node->buffer, block.byte_offset);
			new_block_ptr += block.byte_offset;
		} else {
			memcpy(new_block_ptr, block_handle->node->buffer, block.count * row_chunk.entry_size);
			new_block_ptr += block.count * row_chunk.entry_size;
		}
		buffer_manager.UnregisterBlock(block.block->BlockId(), true);
	}
	row_chunk.blocks.clear();
	row_chunk.block_capacity = capacity;
	row_chunk.blocks.push_back(move(new_block));
}

void SizesToOffsets(BufferManager &buffer_manager, RowChunk &row_chunk, idx_t capacity) {
	RowDataBlock new_block(buffer_manager, capacity, row_chunk.entry_size);
	new_block.count = row_chunk.count;
	auto new_block_handle = buffer_manager.Pin(new_block.block);
	data_ptr_t new_block_ptr = new_block_handle->node->buffer;
	for (auto &block : row_chunk.blocks) {
		auto block_handle = buffer_manager.Pin(block.block);
		memcpy(new_block_ptr, block_handle->node->buffer, block.count * row_chunk.entry_size);
		new_block_ptr += block.count * row_chunk.entry_size;
		buffer_manager.UnregisterBlock(block.block->BlockId(), true);
	}
	row_chunk.blocks.clear();
	row_chunk.block_capacity = capacity;
	// convert sizes to offsets
	idx_t *offsets = (idx_t *)new_block_handle->node->buffer;
	idx_t prev = offsets[0];
	offsets[0] = 0;
	idx_t curr;
	for (idx_t i = 1; i < row_chunk.count; i++) {
		curr = offsets[i];
		offsets[i] = offsets[i - 1] + prev;
		prev = curr;
	}
	offsets[row_chunk.count] = offsets[row_chunk.count - 1] + prev;
	row_chunk.blocks.push_back(move(new_block));
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

	idx_t total_size = 0;
	if (payload_state.HAS_VARIABLE_SIZE) {
		for (auto &block : state.payload_block->blocks) {
			total_size += block.byte_offset;
		}
	} else {
		total_size = state.payload_block->count * payload_state.ENTRY_SIZE;
	}
	if (total_size > state.buffer_manager.GetMaxMemory() / 2) {
		throw NotImplementedException("External sort");
	}

	if (state.sorting_block->blocks.size() > 1) {
		// copy all of the sorting data to one big block
		idx_t capacity = MaxValue(Storage::BLOCK_ALLOC_SIZE / sorting_state.ENTRY_SIZE + 1, state.sorting_block->count);
		ConcatenateBlocks(state.buffer_manager, *state.sorting_block, capacity, false);
	}

	for (idx_t i = 0; i < state.var_sorting_blocks.size(); i++) {
		// copy variable size columns to one big block
		if (!state.var_sorting_blocks[i]) {
			continue;
		}
		auto &row_chunk = *state.var_sorting_blocks[i];
		idx_t var_block_size = 0;
		for (auto &block : row_chunk.blocks) {
			var_block_size += block.byte_offset;
		}
		// variable size data
		idx_t capacity =
		    MaxValue(Storage::BLOCK_ALLOC_SIZE / row_chunk.entry_size + 1, var_block_size / row_chunk.entry_size + 1);
		if (row_chunk.blocks.size() > 1) {
			ConcatenateBlocks(state.buffer_manager, row_chunk, capacity, true);
		}
		// offsets
		auto &sizes_chunk = *state.var_sorting_sizes[i];
		capacity = MaxValue(Storage::BLOCK_ALLOC_SIZE / sizes_chunk.entry_size + 1, sizes_chunk.count + 1);
		SizesToOffsets(state.buffer_manager, sizes_chunk, capacity);
	}

	if (state.payload_block->blocks.size() > 1) {
		// same for the payload data, beware of variable entry size
		idx_t capacity =
		    payload_state.HAS_VARIABLE_SIZE
		        ? MaxValue(Storage::BLOCK_ALLOC_SIZE / payload_state.ENTRY_SIZE + 1,
		                   total_size / payload_state.ENTRY_SIZE + 1)
		        : MaxValue(Storage::BLOCK_ALLOC_SIZE / payload_state.ENTRY_SIZE + 1, state.payload_block->count);
		ConcatenateBlocks(state.buffer_manager, *state.payload_block, capacity, payload_state.HAS_VARIABLE_SIZE);
	}

	if (payload_state.HAS_VARIABLE_SIZE) {
		D_ASSERT(state.sizes_block->count == state.sorting_block->count);
		idx_t capacity =
		    MaxValue(Storage::BLOCK_ALLOC_SIZE / state.sizes_block->entry_size + 1, state.sizes_block->count + 1);
		SizesToOffsets(state.buffer_manager, *state.sizes_block, capacity);
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
			state.key_locations[i] = state.validitymask_locations[i] + payload_state.VALIDITYMASK_SIZE;
			sort_dataptr += sorting_state.ENTRY_SIZE;
		}
	} else {
		for (idx_t i = 0; i < next; i++) {
			state.validitymask_locations[i] = payl_dataptr + Load<idx_t>(sort_dataptr) * payload_state.ENTRY_SIZE;
			state.key_locations[i] = state.validitymask_locations[i] + payload_state.VALIDITYMASK_SIZE;
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
