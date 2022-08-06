#include "duckdb/common/types/row_data_collection_scanner.hpp"

#include "duckdb/common/types/row_data_collection.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

void RowDataCollectionScanner::SwizzleBlocks(RowDataCollection &swizzled_block_collection,
                                             RowDataCollection &swizzled_string_heap,
                                             RowDataCollection &block_collection, RowDataCollection &string_heap,
                                             const RowLayout &layout) {
	if (block_collection.count == 0) {
		return;
	}

	// The main data blocks can just be moved
	swizzled_block_collection.Merge(block_collection);
	block_collection.Clear();

	if (layout.AllConstant()) {
		// No heap blocks!
		return;
	}

	// We create one heap block per data block and swizzle the pointers
	auto &buffer_manager = swizzled_block_collection.buffer_manager;
	auto &heap_blocks = string_heap.blocks;
	idx_t heap_block_idx = 0;
	idx_t heap_block_remaining = heap_blocks[heap_block_idx].count;
	for (auto &data_block : swizzled_block_collection.blocks) {
		if (heap_block_remaining == 0) {
			heap_block_remaining = heap_blocks[++heap_block_idx].count;
		}

		// Pin the data block and swizzle the pointers within the rows
		auto data_handle = buffer_manager.Pin(data_block.block);
		auto data_ptr = data_handle.Ptr();
		RowOperations::SwizzleColumns(layout, data_ptr, data_block.count);

		// We want to copy as little of the heap data as possible, check how the data and heap blocks line up
		if (heap_block_remaining >= data_block.count) {
			// Easy: current heap block contains all strings for this data block, just copy (reference) the block
			swizzled_string_heap.blocks.emplace_back(RowDataBlock(heap_blocks[heap_block_idx]));
			swizzled_string_heap.blocks.back().count = 0;

			// Swizzle the heap pointer
			auto heap_handle = buffer_manager.Pin(swizzled_string_heap.blocks.back().block);
			auto heap_ptr = Load<data_ptr_t>(data_ptr + layout.GetHeapPointerOffset());
			auto heap_offset = heap_ptr - heap_handle.Ptr();
			RowOperations::SwizzleHeapPointer(layout, data_ptr, heap_ptr, data_block.count, heap_offset);

			// Update counter
			heap_block_remaining -= data_block.count;
		} else {
			// Strings for this data block are spread over the current heap block and the next (and possibly more)
			idx_t data_block_remaining = data_block.count;
			vector<std::pair<data_ptr_t, idx_t>> ptrs_and_sizes;
			idx_t total_size = 0;
			while (data_block_remaining > 0) {
				if (heap_block_remaining == 0) {
					heap_block_remaining = heap_blocks[++heap_block_idx].count;
				}
				auto next = MinValue<idx_t>(data_block_remaining, heap_block_remaining);

				// Figure out where to start copying strings, and how many bytes we need to copy
				auto heap_start_ptr = Load<data_ptr_t>(data_ptr + layout.GetHeapPointerOffset());
				auto heap_end_ptr =
				    Load<data_ptr_t>(data_ptr + layout.GetHeapPointerOffset() + (next - 1) * layout.GetRowWidth());
				idx_t size = heap_end_ptr - heap_start_ptr + Load<uint32_t>(heap_end_ptr);
				ptrs_and_sizes.emplace_back(heap_start_ptr, size);
				D_ASSERT(size <= heap_blocks[heap_block_idx].byte_offset);

				// Swizzle the heap pointer
				RowOperations::SwizzleHeapPointer(layout, data_ptr, heap_start_ptr, next, total_size);
				total_size += size;

				// Update where we are in the data and heap blocks
				data_ptr += next * layout.GetRowWidth();
				data_block_remaining -= next;
				heap_block_remaining -= next;
			}

			// Finally, we allocate a new heap block and copy data to it
			swizzled_string_heap.blocks.emplace_back(
			    RowDataBlock(buffer_manager, MaxValue<idx_t>(total_size, (idx_t)Storage::BLOCK_SIZE), 1));
			auto new_heap_handle = buffer_manager.Pin(swizzled_string_heap.blocks.back().block);
			auto new_heap_ptr = new_heap_handle.Ptr();
			for (auto &ptr_and_size : ptrs_and_sizes) {
				memcpy(new_heap_ptr, ptr_and_size.first, ptr_and_size.second);
				new_heap_ptr += ptr_and_size.second;
			}
		}
	}
	D_ASSERT(swizzled_block_collection.blocks.size() == swizzled_string_heap.blocks.size());

	// Update counts and cleanup
	swizzled_string_heap.count = string_heap.count;
	string_heap.Clear();
}

void RowDataCollectionScanner::ScanState::PinData() {
	auto &rows = scanner.rows;
	D_ASSERT(block_idx < rows.blocks.size());
	auto &data_block = rows.blocks[block_idx];
	if (!data_handle.IsValid() || data_handle.GetBlockId() != data_block.block->BlockId()) {
		data_handle = rows.buffer_manager.Pin(data_block.block);
	}
	if (scanner.layout.AllConstant()) {
		return;
	}

	auto &heap = scanner.heap;
	D_ASSERT(block_idx < heap.blocks.size());
	auto &heap_block = heap.blocks[block_idx];
	if (!heap_handle.IsValid() || heap_handle.GetBlockId() != heap_block.block->BlockId()) {
		heap_handle = heap.buffer_manager.Pin(heap_block.block);
	}
}

RowDataCollectionScanner::RowDataCollectionScanner(RowDataCollection &rows_p, RowDataCollection &heap_p,
                                                   const RowLayout &layout_p, bool flush_p)
    : rows(rows_p), heap(heap_p), layout(layout_p), read_state(*this), total_count(rows.count), total_scanned(0),
      flush(flush_p) {

	if (!layout.AllConstant()) {
		D_ASSERT(rows.blocks.size() == heap.blocks.size());
	}
}

void RowDataCollectionScanner::Scan(DataChunk &chunk) {
	auto count = MinValue((idx_t)STANDARD_VECTOR_SIZE, total_count - total_scanned);
	if (count == 0) {
		chunk.SetCardinality(count);
		return;
	}
	// Eagerly delete references to blocks that we've passed
	if (flush) {
		for (idx_t i = 0; i < read_state.block_idx; ++i) {
			rows.blocks[i].block = nullptr;
			if (!layout.AllConstant()) {
				heap.blocks[i].block = nullptr;
			}
		}
	}
	const idx_t &row_width = layout.GetRowWidth();
	// Set up a batch of pointers to scan data from
	idx_t scanned = 0;
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);
	while (scanned < count) {
		read_state.PinData();
		auto &data_block = rows.blocks[read_state.block_idx];
		idx_t next = MinValue(data_block.count - read_state.entry_idx, count - scanned);
		const data_ptr_t data_ptr = read_state.data_handle.Ptr() + read_state.entry_idx * row_width;
		// Set up the next pointers
		data_ptr_t row_ptr = data_ptr;
		for (idx_t i = 0; i < next; i++) {
			data_pointers[scanned + i] = row_ptr;
			row_ptr += row_width;
		}
		// Unswizzle the offsets back to pointers (if needed)
		if (!layout.AllConstant()) {
			RowOperations::UnswizzlePointers(layout, data_ptr, read_state.heap_handle.Ptr(), next);
		}
		// Update state indices
		read_state.entry_idx += next;
		if (read_state.entry_idx == data_block.count) {
			read_state.block_idx++;
			read_state.entry_idx = 0;
		}
		scanned += next;
	}
	D_ASSERT(scanned == count);
	// Deserialize the payload data
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		const auto col_offset = layout.GetOffsets()[col_idx];
		RowOperations::Gather(addresses, *FlatVector::IncrementalSelectionVector(), chunk.data[col_idx],
		                      *FlatVector::IncrementalSelectionVector(), count, col_offset, col_idx);
	}
	chunk.SetCardinality(count);
	chunk.Verify();
	total_scanned += scanned;
}

} // namespace duckdb
