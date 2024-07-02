#include "duckdb/common/types/row/row_data_collection_scanner.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row/row_data_collection.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <numeric>

namespace duckdb {

void RowDataCollectionScanner::AlignHeapBlocks(RowDataCollection &swizzled_block_collection,
                                               RowDataCollection &swizzled_string_heap,
                                               RowDataCollection &block_collection, RowDataCollection &string_heap,
                                               const RowLayout &layout) {
	if (block_collection.count == 0) {
		return;
	}

	if (layout.AllConstant()) {
		// No heap blocks! Just merge fixed-size data
		swizzled_block_collection.Merge(block_collection);
		return;
	}

	// We create one heap block per data block and swizzle the pointers
	D_ASSERT(string_heap.keep_pinned == swizzled_string_heap.keep_pinned);
	auto &buffer_manager = block_collection.buffer_manager;
	auto &heap_blocks = string_heap.blocks;
	idx_t heap_block_idx = 0;
	idx_t heap_block_remaining = heap_blocks[heap_block_idx]->count;
	for (auto &data_block : block_collection.blocks) {
		if (heap_block_remaining == 0) {
			heap_block_remaining = heap_blocks[++heap_block_idx]->count;
		}

		// Pin the data block and swizzle the pointers within the rows
		auto data_handle = buffer_manager.Pin(data_block->block);
		auto data_ptr = data_handle.Ptr();
		if (!string_heap.keep_pinned) {
			D_ASSERT(!data_block->block->IsSwizzled());
			RowOperations::SwizzleColumns(layout, data_ptr, data_block->count);
			data_block->block->SetSwizzling(nullptr);
		}
		// At this point the data block is pinned and the heap pointer is valid
		// so we can copy heap data as needed

		// We want to copy as little of the heap data as possible, check how the data and heap blocks line up
		if (heap_block_remaining >= data_block->count) {
			// Easy: current heap block contains all strings for this data block, just copy (reference) the block
			swizzled_string_heap.blocks.emplace_back(heap_blocks[heap_block_idx]->Copy());
			swizzled_string_heap.blocks.back()->count = data_block->count;

			// Swizzle the heap pointer if we are not pinning the heap
			auto &heap_block = swizzled_string_heap.blocks.back()->block;
			auto heap_handle = buffer_manager.Pin(heap_block);
			if (!swizzled_string_heap.keep_pinned) {
				auto heap_ptr = Load<data_ptr_t>(data_ptr + layout.GetHeapOffset());
				auto heap_offset = heap_ptr - heap_handle.Ptr();
				RowOperations::SwizzleHeapPointer(layout, data_ptr, heap_ptr, data_block->count,
				                                  NumericCast<idx_t>(heap_offset));
			} else {
				swizzled_string_heap.pinned_blocks.emplace_back(std::move(heap_handle));
			}

			// Update counter
			heap_block_remaining -= data_block->count;
		} else {
			// Strings for this data block are spread over the current heap block and the next (and possibly more)
			if (string_heap.keep_pinned) {
				// The heap is changing underneath the data block,
				// so swizzle the string pointers to make them portable.
				RowOperations::SwizzleColumns(layout, data_ptr, data_block->count);
			}
			idx_t data_block_remaining = data_block->count;
			vector<std::pair<data_ptr_t, idx_t>> ptrs_and_sizes;
			idx_t total_size = 0;
			const auto base_row_ptr = data_ptr;
			while (data_block_remaining > 0) {
				if (heap_block_remaining == 0) {
					heap_block_remaining = heap_blocks[++heap_block_idx]->count;
				}
				auto next = MinValue<idx_t>(data_block_remaining, heap_block_remaining);

				// Figure out where to start copying strings, and how many bytes we need to copy
				auto heap_start_ptr = Load<data_ptr_t>(data_ptr + layout.GetHeapOffset());
				auto heap_end_ptr =
				    Load<data_ptr_t>(data_ptr + layout.GetHeapOffset() + (next - 1) * layout.GetRowWidth());
				auto size = NumericCast<idx_t>(heap_end_ptr - heap_start_ptr + Load<uint32_t>(heap_end_ptr));
				ptrs_and_sizes.emplace_back(heap_start_ptr, size);
				D_ASSERT(size <= heap_blocks[heap_block_idx]->byte_offset);

				// Swizzle the heap pointer
				RowOperations::SwizzleHeapPointer(layout, data_ptr, heap_start_ptr, next, total_size);
				total_size += size;

				// Update where we are in the data and heap blocks
				data_ptr += next * layout.GetRowWidth();
				data_block_remaining -= next;
				heap_block_remaining -= next;
			}

			// Finally, we allocate a new heap block and copy data to it
			swizzled_string_heap.blocks.emplace_back(make_uniq<RowDataBlock>(
			    MemoryTag::ORDER_BY, buffer_manager, MaxValue<idx_t>(total_size, buffer_manager.GetBlockSize()), 1U));
			auto new_heap_handle = buffer_manager.Pin(swizzled_string_heap.blocks.back()->block);
			auto new_heap_ptr = new_heap_handle.Ptr();
			for (auto &ptr_and_size : ptrs_and_sizes) {
				memcpy(new_heap_ptr, ptr_and_size.first, ptr_and_size.second);
				new_heap_ptr += ptr_and_size.second;
			}
			new_heap_ptr = new_heap_handle.Ptr();
			if (swizzled_string_heap.keep_pinned) {
				// Since the heap blocks are pinned, we can unswizzle the data again.
				swizzled_string_heap.pinned_blocks.emplace_back(std::move(new_heap_handle));
				RowOperations::UnswizzlePointers(layout, base_row_ptr, new_heap_ptr, data_block->count);
				RowOperations::UnswizzleHeapPointer(layout, base_row_ptr, new_heap_ptr, data_block->count);
			}
		}
	}

	// We're done with variable-sized data, now just merge the fixed-size data
	swizzled_block_collection.Merge(block_collection);
	D_ASSERT(swizzled_block_collection.blocks.size() == swizzled_string_heap.blocks.size());

	// Update counts and cleanup
	swizzled_string_heap.count = string_heap.count;
	string_heap.Clear();
}

void RowDataCollectionScanner::ScanState::PinData() {
	auto &rows = scanner.rows;
	D_ASSERT(block_idx < rows.blocks.size());
	auto &data_block = rows.blocks[block_idx];
	if (!data_handle.IsValid() || data_handle.GetBlockHandle() != data_block->block) {
		data_handle = rows.buffer_manager.Pin(data_block->block);
	}
	if (scanner.layout.AllConstant() || !scanner.external) {
		return;
	}

	auto &heap = scanner.heap;
	D_ASSERT(block_idx < heap.blocks.size());
	auto &heap_block = heap.blocks[block_idx];
	if (!heap_handle.IsValid() || heap_handle.GetBlockHandle() != heap_block->block) {
		heap_handle = heap.buffer_manager.Pin(heap_block->block);
	}
}

RowDataCollectionScanner::RowDataCollectionScanner(RowDataCollection &rows_p, RowDataCollection &heap_p,
                                                   const RowLayout &layout_p, bool external_p, bool flush_p)
    : rows(rows_p), heap(heap_p), layout(layout_p), read_state(*this), total_count(rows.count), total_scanned(0),
      external(external_p), flush(flush_p), unswizzling(!layout.AllConstant() && external && !heap.keep_pinned) {

	if (unswizzling) {
		D_ASSERT(rows.blocks.size() == heap.blocks.size());
	}

	ValidateUnscannedBlock();
}

RowDataCollectionScanner::RowDataCollectionScanner(RowDataCollection &rows_p, RowDataCollection &heap_p,
                                                   const RowLayout &layout_p, bool external_p, idx_t block_idx,
                                                   bool flush_p)
    : rows(rows_p), heap(heap_p), layout(layout_p), read_state(*this), total_count(rows.count), total_scanned(0),
      external(external_p), flush(flush_p), unswizzling(!layout.AllConstant() && external && !heap.keep_pinned) {

	if (unswizzling) {
		D_ASSERT(rows.blocks.size() == heap.blocks.size());
	}

	D_ASSERT(block_idx < rows.blocks.size());
	read_state.block_idx = block_idx;
	read_state.entry_idx = 0;

	//	Pretend that we have scanned up to the start block
	//	and will stop at the end
	auto begin = rows.blocks.begin();
	auto end = begin + NumericCast<int64_t>(block_idx);
	total_scanned =
	    std::accumulate(begin, end, idx_t(0), [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });
	total_count = total_scanned + (*end)->count;

	ValidateUnscannedBlock();
}

void RowDataCollectionScanner::SwizzleBlock(RowDataBlock &data_block, RowDataBlock &heap_block) {
	// Pin the data block and swizzle the pointers within the rows
	D_ASSERT(!data_block.block->IsSwizzled());
	auto data_handle = rows.buffer_manager.Pin(data_block.block);
	auto data_ptr = data_handle.Ptr();
	RowOperations::SwizzleColumns(layout, data_ptr, data_block.count);
	data_block.block->SetSwizzling(nullptr);

	// Swizzle the heap pointers
	auto heap_handle = heap.buffer_manager.Pin(heap_block.block);
	auto heap_ptr = Load<data_ptr_t>(data_ptr + layout.GetHeapOffset());
	auto heap_offset = heap_ptr - heap_handle.Ptr();
	RowOperations::SwizzleHeapPointer(layout, data_ptr, heap_ptr, data_block.count, NumericCast<idx_t>(heap_offset));
}

void RowDataCollectionScanner::ReSwizzle() {
	if (rows.count == 0) {
		return;
	}

	if (!unswizzling) {
		// No swizzled blocks!
		return;
	}

	D_ASSERT(rows.blocks.size() == heap.blocks.size());
	for (idx_t i = 0; i < rows.blocks.size(); ++i) {
		auto &data_block = rows.blocks[i];
		if (data_block->block && !data_block->block->IsSwizzled()) {
			SwizzleBlock(*data_block, *heap.blocks[i]);
		}
	}
}

void RowDataCollectionScanner::ValidateUnscannedBlock() const {
	if (unswizzling && read_state.block_idx < rows.blocks.size() && Remaining()) {
		D_ASSERT(rows.blocks[read_state.block_idx]->block->IsSwizzled());
	}
}

void RowDataCollectionScanner::Scan(DataChunk &chunk) {
	auto count = MinValue((idx_t)STANDARD_VECTOR_SIZE, total_count - total_scanned);
	if (count == 0) {
		chunk.SetCardinality(count);
		return;
	}

	//	Only flush blocks we processed.
	const auto flush_block_idx = read_state.block_idx;

	const idx_t &row_width = layout.GetRowWidth();
	// Set up a batch of pointers to scan data from
	idx_t scanned = 0;
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	// We must pin ALL blocks we are going to gather from
	vector<BufferHandle> pinned_blocks;
	while (scanned < count) {
		read_state.PinData();
		auto &data_block = rows.blocks[read_state.block_idx];
		idx_t next = MinValue(data_block->count - read_state.entry_idx, count - scanned);
		const data_ptr_t data_ptr = read_state.data_handle.Ptr() + read_state.entry_idx * row_width;
		// Set up the next pointers
		data_ptr_t row_ptr = data_ptr;
		for (idx_t i = 0; i < next; i++) {
			data_pointers[scanned + i] = row_ptr;
			row_ptr += row_width;
		}
		// Unswizzle the offsets back to pointers (if needed)
		if (unswizzling) {
			RowOperations::UnswizzlePointers(layout, data_ptr, read_state.heap_handle.Ptr(), next);
			rows.blocks[read_state.block_idx]->block->SetSwizzling("RowDataCollectionScanner::Scan");
		}
		// Update state indices
		read_state.entry_idx += next;
		scanned += next;
		total_scanned += next;
		if (read_state.entry_idx == data_block->count) {
			// Pin completed blocks so we don't lose them
			pinned_blocks.emplace_back(rows.buffer_manager.Pin(data_block->block));
			if (unswizzling) {
				auto &heap_block = heap.blocks[read_state.block_idx];
				pinned_blocks.emplace_back(heap.buffer_manager.Pin(heap_block->block));
			}
			read_state.block_idx++;
			read_state.entry_idx = 0;
			ValidateUnscannedBlock();
		}
	}
	D_ASSERT(scanned == count);
	// Deserialize the payload data
	for (idx_t col_no = 0; col_no < layout.ColumnCount(); col_no++) {
		RowOperations::Gather(addresses, *FlatVector::IncrementalSelectionVector(), chunk.data[col_no],
		                      *FlatVector::IncrementalSelectionVector(), count, layout, col_no);
	}
	chunk.SetCardinality(count);
	chunk.Verify();

	//	Switch to a new set of pinned blocks
	read_state.pinned_blocks.swap(pinned_blocks);

	if (flush) {
		// Release blocks we have passed.
		for (idx_t i = flush_block_idx; i < read_state.block_idx; ++i) {
			rows.blocks[i]->block = nullptr;
			if (unswizzling) {
				heap.blocks[i]->block = nullptr;
			}
		}
	} else if (unswizzling) {
		// Reswizzle blocks we have passed so they can be flushed safely.
		for (idx_t i = flush_block_idx; i < read_state.block_idx; ++i) {
			auto &data_block = rows.blocks[i];
			if (data_block->block && !data_block->block->IsSwizzled()) {
				SwizzleBlock(*data_block, *heap.blocks[i]);
			}
		}
	}
}

void RowDataCollectionScanner::Reset(bool flush_p) {
	flush = flush_p;
	total_scanned = 0;

	read_state.block_idx = 0;
	read_state.entry_idx = 0;
}

} // namespace duckdb
