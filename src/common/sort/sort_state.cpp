#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/sort/sorted_block.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

#include <algorithm>
#include <numeric>

namespace duckdb {

idx_t GetNestedSortingColSize(idx_t &col_size, const LogicalType &type) {
	auto physical_type = type.InternalType();
	if (TypeIsConstantSize(physical_type)) {
		col_size += GetTypeIdSize(physical_type);
		return 0;
	} else {
		switch (physical_type) {
		case PhysicalType::VARCHAR: {
			// Nested strings are between 4 and 11 chars long for alignment
			auto size_before_str = col_size;
			col_size += 11;
			col_size -= (col_size - 12) % 8;
			return col_size - size_before_str;
		}
		case PhysicalType::LIST:
			// Lists get 2 bytes (null and empty list)
			col_size += 2;
			return GetNestedSortingColSize(col_size, ListType::GetChildType(type));
		case PhysicalType::STRUCT:
			// Structs get 1 bytes (null)
			col_size++;
			return GetNestedSortingColSize(col_size, StructType::GetChildType(type, 0));
		case PhysicalType::ARRAY:
			// Arrays get 1 bytes (null)
			col_size++;
			return GetNestedSortingColSize(col_size, ArrayType::GetChildType(type));
		default:
			throw NotImplementedException("Unable to order column with type %s", type.ToString());
		}
	}
}

SortLayout::SortLayout(const vector<BoundOrderByNode> &orders)
    : column_count(orders.size()), all_constant(true), comparison_size(0), entry_size(0) {
	vector<LogicalType> blob_layout_types;
	for (idx_t i = 0; i < column_count; i++) {
		const auto &order = orders[i];

		order_types.push_back(order.type);
		order_by_null_types.push_back(order.null_order);
		auto &expr = *order.expression;
		logical_types.push_back(expr.return_type);

		auto physical_type = expr.return_type.InternalType();
		constant_size.push_back(TypeIsConstantSize(physical_type));

		if (order.stats) {
			stats.push_back(order.stats.get());
			has_null.push_back(stats.back()->CanHaveNull());
		} else {
			stats.push_back(nullptr);
			has_null.push_back(true);
		}

		idx_t col_size = has_null.back() ? 1 : 0;
		prefix_lengths.push_back(0);
		if (!TypeIsConstantSize(physical_type) && physical_type != PhysicalType::VARCHAR) {
			prefix_lengths.back() = GetNestedSortingColSize(col_size, expr.return_type);
		} else if (physical_type == PhysicalType::VARCHAR) {
			idx_t size_before = col_size;
			if (stats.back() && StringStats::HasMaxStringLength(*stats.back())) {
				col_size += StringStats::MaxStringLength(*stats.back());
				if (col_size > 12) {
					col_size = 12;
				} else {
					constant_size.back() = true;
				}
			} else {
				col_size = 12;
			}
			prefix_lengths.back() = col_size - size_before;
		} else {
			col_size += GetTypeIdSize(physical_type);
		}

		comparison_size += col_size;
		column_sizes.push_back(col_size);
	}
	entry_size = comparison_size + sizeof(uint32_t);

	// 8-byte alignment
	if (entry_size % 8 != 0) {
		// First assign more bytes to strings instead of aligning
		idx_t bytes_to_fill = 8 - (entry_size % 8);
		for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
			if (bytes_to_fill == 0) {
				break;
			}
			if (logical_types[col_idx].InternalType() == PhysicalType::VARCHAR && stats[col_idx] &&
			    StringStats::HasMaxStringLength(*stats[col_idx])) {
				idx_t diff = StringStats::MaxStringLength(*stats[col_idx]) - prefix_lengths[col_idx];
				if (diff > 0) {
					// Increase all sizes accordingly
					idx_t increase = MinValue(bytes_to_fill, diff);
					column_sizes[col_idx] += increase;
					prefix_lengths[col_idx] += increase;
					constant_size[col_idx] = increase == diff;
					comparison_size += increase;
					entry_size += increase;
					bytes_to_fill -= increase;
				}
			}
		}
		entry_size = AlignValue(entry_size);
	}

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		all_constant = all_constant && constant_size[col_idx];
		if (!constant_size[col_idx]) {
			sorting_to_blob_col[col_idx] = blob_layout_types.size();
			blob_layout_types.push_back(logical_types[col_idx]);
		}
	}

	blob_layout.Initialize(blob_layout_types);
}

SortLayout SortLayout::GetPrefixComparisonLayout(idx_t num_prefix_cols) const {
	SortLayout result;
	result.column_count = num_prefix_cols;
	result.all_constant = true;
	result.comparison_size = 0;
	for (idx_t col_idx = 0; col_idx < num_prefix_cols; col_idx++) {
		result.order_types.push_back(order_types[col_idx]);
		result.order_by_null_types.push_back(order_by_null_types[col_idx]);
		result.logical_types.push_back(logical_types[col_idx]);

		result.all_constant = result.all_constant && constant_size[col_idx];
		result.constant_size.push_back(constant_size[col_idx]);

		result.comparison_size += column_sizes[col_idx];
		result.column_sizes.push_back(column_sizes[col_idx]);

		result.prefix_lengths.push_back(prefix_lengths[col_idx]);
		result.stats.push_back(stats[col_idx]);
		result.has_null.push_back(has_null[col_idx]);
	}
	result.entry_size = entry_size;
	result.blob_layout = blob_layout;
	result.sorting_to_blob_col = sorting_to_blob_col;
	return result;
}

LocalSortState::LocalSortState() : initialized(false) {
	if (!Radix::IsLittleEndian()) {
		throw NotImplementedException("Sorting is not supported on big endian architectures");
	}
}

void LocalSortState::Initialize(GlobalSortState &global_sort_state, BufferManager &buffer_manager_p) {
	sort_layout = &global_sort_state.sort_layout;
	payload_layout = &global_sort_state.payload_layout;
	buffer_manager = &buffer_manager_p;
	const auto block_size = buffer_manager->GetBlockSize();

	// Radix sorting data
	auto entries_per_block = RowDataCollection::EntriesPerBlock(sort_layout->entry_size, block_size);
	radix_sorting_data = make_uniq<RowDataCollection>(*buffer_manager, entries_per_block, sort_layout->entry_size);

	// Blob sorting data
	if (!sort_layout->all_constant) {
		auto blob_row_width = sort_layout->blob_layout.GetRowWidth();
		entries_per_block = RowDataCollection::EntriesPerBlock(blob_row_width, block_size);
		blob_sorting_data = make_uniq<RowDataCollection>(*buffer_manager, entries_per_block, blob_row_width);
		blob_sorting_heap = make_uniq<RowDataCollection>(*buffer_manager, block_size, 1U, true);
	}

	// Payload data
	auto payload_row_width = payload_layout->GetRowWidth();
	entries_per_block = RowDataCollection::EntriesPerBlock(payload_row_width, block_size);
	payload_data = make_uniq<RowDataCollection>(*buffer_manager, entries_per_block, payload_row_width);
	payload_heap = make_uniq<RowDataCollection>(*buffer_manager, block_size, 1U, true);
	initialized = true;
}

void LocalSortState::SinkChunk(DataChunk &sort, DataChunk &payload) {
	D_ASSERT(sort.size() == payload.size());
	// Build and serialize sorting data to radix sortable rows
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);
	auto handles = radix_sorting_data->Build(sort.size(), data_pointers, nullptr);
	for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
		bool has_null = sort_layout->has_null[sort_col];
		bool nulls_first = sort_layout->order_by_null_types[sort_col] == OrderByNullType::NULLS_FIRST;
		bool desc = sort_layout->order_types[sort_col] == OrderType::DESCENDING;
		RowOperations::RadixScatter(sort.data[sort_col], sort.size(), sel_ptr, sort.size(), data_pointers, desc,
		                            has_null, nulls_first, sort_layout->prefix_lengths[sort_col],
		                            sort_layout->column_sizes[sort_col]);
	}

	// Also fully serialize blob sorting columns (to be able to break ties
	if (!sort_layout->all_constant) {
		DataChunk blob_chunk;
		blob_chunk.SetCardinality(sort.size());
		for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
			if (!sort_layout->constant_size[sort_col]) {
				blob_chunk.data.emplace_back(sort.data[sort_col]);
			}
		}
		handles = blob_sorting_data->Build(blob_chunk.size(), data_pointers, nullptr);
		auto blob_data = blob_chunk.ToUnifiedFormat();
		RowOperations::Scatter(blob_chunk, blob_data.get(), sort_layout->blob_layout, addresses, *blob_sorting_heap,
		                       sel_ptr, blob_chunk.size());
		D_ASSERT(blob_sorting_heap->keep_pinned);
	}

	// Finally, serialize payload data
	handles = payload_data->Build(payload.size(), data_pointers, nullptr);
	auto input_data = payload.ToUnifiedFormat();
	RowOperations::Scatter(payload, input_data.get(), *payload_layout, addresses, *payload_heap, sel_ptr,
	                       payload.size());
	D_ASSERT(payload_heap->keep_pinned);
}

idx_t LocalSortState::SizeInBytes() const {
	idx_t size_in_bytes = radix_sorting_data->SizeInBytes() + payload_data->SizeInBytes();
	if (!sort_layout->all_constant) {
		size_in_bytes += blob_sorting_data->SizeInBytes() + blob_sorting_heap->SizeInBytes();
	}
	if (!payload_layout->AllConstant()) {
		size_in_bytes += payload_heap->SizeInBytes();
	}
	return size_in_bytes;
}

void LocalSortState::Sort(GlobalSortState &global_sort_state, bool reorder_heap) {
	D_ASSERT(radix_sorting_data->count == payload_data->count);
	if (radix_sorting_data->count == 0) {
		return;
	}
	// Move all data to a single SortedBlock
	sorted_blocks.emplace_back(make_uniq<SortedBlock>(*buffer_manager, global_sort_state));
	auto &sb = *sorted_blocks.back();
	// Fixed-size sorting data
	auto sorting_block = ConcatenateBlocks(*radix_sorting_data);
	sb.radix_sorting_data.push_back(std::move(sorting_block));
	// Variable-size sorting data
	if (!sort_layout->all_constant) {
		auto &blob_data = *blob_sorting_data;
		auto new_block = ConcatenateBlocks(blob_data);
		sb.blob_sorting_data->data_blocks.push_back(std::move(new_block));
	}
	// Payload data
	auto payload_block = ConcatenateBlocks(*payload_data);
	sb.payload_data->data_blocks.push_back(std::move(payload_block));
	// Now perform the actual sort
	SortInMemory();
	// Re-order before the merge sort
	ReOrder(global_sort_state, reorder_heap);
}

unique_ptr<RowDataBlock> LocalSortState::ConcatenateBlocks(RowDataCollection &row_data) {
	//	Don't copy and delete if there is only one block.
	if (row_data.blocks.size() == 1) {
		auto new_block = std::move(row_data.blocks[0]);
		row_data.blocks.clear();
		row_data.count = 0;
		return new_block;
	}
	// Create block with the correct capacity
	auto &buffer_manager = row_data.buffer_manager;
	const idx_t &entry_size = row_data.entry_size;
	idx_t capacity = MaxValue((buffer_manager.GetBlockSize() + entry_size - 1) / entry_size, row_data.count);
	auto new_block = make_uniq<RowDataBlock>(MemoryTag::ORDER_BY, buffer_manager, capacity, entry_size);
	new_block->count = row_data.count;
	auto new_block_handle = buffer_manager.Pin(new_block->block);
	data_ptr_t new_block_ptr = new_block_handle.Ptr();
	// Copy the data of the blocks into a single block
	for (idx_t i = 0; i < row_data.blocks.size(); i++) {
		auto &block = row_data.blocks[i];
		auto block_handle = buffer_manager.Pin(block->block);
		memcpy(new_block_ptr, block_handle.Ptr(), block->count * entry_size);
		new_block_ptr += block->count * entry_size;
		block.reset();
	}
	row_data.blocks.clear();
	row_data.count = 0;
	return new_block;
}

void LocalSortState::ReOrder(SortedData &sd, data_ptr_t sorting_ptr, RowDataCollection &heap, GlobalSortState &gstate,
                             bool reorder_heap) {
	sd.swizzled = reorder_heap;
	auto &unordered_data_block = sd.data_blocks.back();
	const idx_t count = unordered_data_block->count;
	auto unordered_data_handle = buffer_manager->Pin(unordered_data_block->block);
	const data_ptr_t unordered_data_ptr = unordered_data_handle.Ptr();
	// Create new block that will hold re-ordered row data
	auto ordered_data_block = make_uniq<RowDataBlock>(MemoryTag::ORDER_BY, *buffer_manager,
	                                                  unordered_data_block->capacity, unordered_data_block->entry_size);
	ordered_data_block->count = count;
	auto ordered_data_handle = buffer_manager->Pin(ordered_data_block->block);
	data_ptr_t ordered_data_ptr = ordered_data_handle.Ptr();
	// Re-order fixed-size row layout
	const idx_t row_width = sd.layout.GetRowWidth();
	const idx_t sorting_entry_size = gstate.sort_layout.entry_size;
	for (idx_t i = 0; i < count; i++) {
		auto index = Load<uint32_t>(sorting_ptr);
		FastMemcpy(ordered_data_ptr, unordered_data_ptr + index * row_width, row_width);
		ordered_data_ptr += row_width;
		sorting_ptr += sorting_entry_size;
	}
	ordered_data_block->block->SetSwizzling(
	    sd.layout.AllConstant() || !sd.swizzled ? nullptr : "LocalSortState::ReOrder.ordered_data");
	// Replace the unordered data block with the re-ordered data block
	sd.data_blocks.clear();
	sd.data_blocks.push_back(std::move(ordered_data_block));
	// Deal with the heap (if necessary)
	if (!sd.layout.AllConstant() && reorder_heap) {
		// Swizzle the column pointers to offsets
		RowOperations::SwizzleColumns(sd.layout, ordered_data_handle.Ptr(), count);
		sd.data_blocks.back()->block->SetSwizzling(nullptr);
		// Create a single heap block to store the ordered heap
		idx_t total_byte_offset =
		    std::accumulate(heap.blocks.begin(), heap.blocks.end(), (idx_t)0,
		                    [](idx_t a, const unique_ptr<RowDataBlock> &b) { return a + b->byte_offset; });
		idx_t heap_block_size = MaxValue(total_byte_offset, buffer_manager->GetBlockSize());
		auto ordered_heap_block = make_uniq<RowDataBlock>(MemoryTag::ORDER_BY, *buffer_manager, heap_block_size, 1U);
		ordered_heap_block->count = count;
		ordered_heap_block->byte_offset = total_byte_offset;
		auto ordered_heap_handle = buffer_manager->Pin(ordered_heap_block->block);
		data_ptr_t ordered_heap_ptr = ordered_heap_handle.Ptr();
		// Fill the heap in order
		ordered_data_ptr = ordered_data_handle.Ptr();
		const idx_t heap_pointer_offset = sd.layout.GetHeapOffset();
		for (idx_t i = 0; i < count; i++) {
			auto heap_row_ptr = Load<data_ptr_t>(ordered_data_ptr + heap_pointer_offset);
			auto heap_row_size = Load<uint32_t>(heap_row_ptr);
			memcpy(ordered_heap_ptr, heap_row_ptr, heap_row_size);
			ordered_heap_ptr += heap_row_size;
			ordered_data_ptr += row_width;
		}
		// Swizzle the base pointer to the offset of each row in the heap
		RowOperations::SwizzleHeapPointer(sd.layout, ordered_data_handle.Ptr(), ordered_heap_handle.Ptr(), count);
		// Move the re-ordered heap to the SortedData, and clear the local heap
		sd.heap_blocks.push_back(std::move(ordered_heap_block));
		heap.pinned_blocks.clear();
		heap.blocks.clear();
		heap.count = 0;
	}
}

void LocalSortState::ReOrder(GlobalSortState &gstate, bool reorder_heap) {
	auto &sb = *sorted_blocks.back();
	auto sorting_handle = buffer_manager->Pin(sb.radix_sorting_data.back()->block);
	const data_ptr_t sorting_ptr = sorting_handle.Ptr() + gstate.sort_layout.comparison_size;
	// Re-order variable size sorting columns
	if (!gstate.sort_layout.all_constant) {
		ReOrder(*sb.blob_sorting_data, sorting_ptr, *blob_sorting_heap, gstate, reorder_heap);
	}
	// And the payload
	ReOrder(*sb.payload_data, sorting_ptr, *payload_heap, gstate, reorder_heap);
}

GlobalSortState::GlobalSortState(ClientContext &context_p, const vector<BoundOrderByNode> &orders,
                                 RowLayout &payload_layout)
    : context(context_p), buffer_manager(BufferManager::GetBufferManager(context)), sort_layout(SortLayout(orders)),
      payload_layout(payload_layout), block_capacity(0), external(false) {
}

void GlobalSortState::AddLocalState(LocalSortState &local_sort_state) {
	if (!local_sort_state.radix_sorting_data) {
		return;
	}

	// Sort accumulated data
	// we only re-order the heap when the data is expected to not fit in memory
	// re-ordering the heap avoids random access when reading/merging but incurs a significant cost of shuffling data
	// when data fits in memory, doing random access on reads is cheaper than re-shuffling
	local_sort_state.Sort(*this, external || !local_sort_state.sorted_blocks.empty());

	// Append local state sorted data to this global state
	lock_guard<mutex> append_guard(lock);
	for (auto &sb : local_sort_state.sorted_blocks) {
		sorted_blocks.push_back(std::move(sb));
	}
	auto &payload_heap = local_sort_state.payload_heap;
	for (idx_t i = 0; i < payload_heap->blocks.size(); i++) {
		heap_blocks.push_back(std::move(payload_heap->blocks[i]));
		pinned_blocks.push_back(std::move(payload_heap->pinned_blocks[i]));
	}
	if (!sort_layout.all_constant) {
		auto &blob_heap = local_sort_state.blob_sorting_heap;
		for (idx_t i = 0; i < blob_heap->blocks.size(); i++) {
			heap_blocks.push_back(std::move(blob_heap->blocks[i]));
			pinned_blocks.push_back(std::move(blob_heap->pinned_blocks[i]));
		}
	}
}

void GlobalSortState::PrepareMergePhase() {
	// Determine if we need to use do an external sort
	idx_t total_heap_size =
	    std::accumulate(sorted_blocks.begin(), sorted_blocks.end(), (idx_t)0,
	                    [](idx_t a, const unique_ptr<SortedBlock> &b) { return a + b->HeapSize(); });
	if (external || (pinned_blocks.empty() && total_heap_size * 4 > buffer_manager.GetQueryMaxMemory())) {
		external = true;
	}
	// Use the data that we have to determine which partition size to use during the merge
	if (external && total_heap_size > 0) {
		// If we have variable size data we need to be conservative, as there might be skew
		idx_t max_block_size = 0;
		for (auto &sb : sorted_blocks) {
			idx_t size_in_bytes = sb->SizeInBytes();
			if (size_in_bytes > max_block_size) {
				max_block_size = size_in_bytes;
				block_capacity = sb->Count();
			}
		}
	} else {
		for (auto &sb : sorted_blocks) {
			block_capacity = MaxValue(block_capacity, sb->Count());
		}
	}
	// Unswizzle and pin heap blocks if we can fit everything in memory
	if (!external) {
		for (auto &sb : sorted_blocks) {
			sb->blob_sorting_data->Unswizzle();
			sb->payload_data->Unswizzle();
		}
	}
}

void GlobalSortState::InitializeMergeRound() {
	D_ASSERT(sorted_blocks_temp.empty());
	// If we reverse this list, the blocks that were merged last will be merged first in the next round
	// These are still in memory, therefore this reduces the amount of read/write to disk!
	std::reverse(sorted_blocks.begin(), sorted_blocks.end());
	// Uneven number of blocks - keep one on the side
	if (sorted_blocks.size() % 2 == 1) {
		odd_one_out = std::move(sorted_blocks.back());
		sorted_blocks.pop_back();
	}
	// Init merge path path indices
	pair_idx = 0;
	num_pairs = sorted_blocks.size() / 2;
	l_start = 0;
	r_start = 0;
	// Allocate room for merge results
	for (idx_t p_idx = 0; p_idx < num_pairs; p_idx++) {
		sorted_blocks_temp.emplace_back();
	}
}

void GlobalSortState::CompleteMergeRound(bool keep_radix_data) {
	sorted_blocks.clear();
	for (auto &sorted_block_vector : sorted_blocks_temp) {
		sorted_blocks.push_back(make_uniq<SortedBlock>(buffer_manager, *this));
		sorted_blocks.back()->AppendSortedBlocks(sorted_block_vector);
	}
	sorted_blocks_temp.clear();
	if (odd_one_out) {
		sorted_blocks.push_back(std::move(odd_one_out));
		odd_one_out = nullptr;
	}
	// Only one block left: Done!
	if (sorted_blocks.size() == 1 && !keep_radix_data) {
		sorted_blocks[0]->radix_sorting_data.clear();
		sorted_blocks[0]->blob_sorting_data = nullptr;
	}
}
void GlobalSortState::Print() {
	PayloadScanner scanner(*this, false);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), scanner.GetPayloadTypes());
	for (;;) {
		scanner.Scan(chunk);
		const auto count = chunk.size();
		if (!count) {
			break;
		}
		chunk.Print();
	}
}

} // namespace duckdb
