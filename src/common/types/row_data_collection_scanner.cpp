#include "duckdb/common/types/row_data_collection_scanner.hpp"

#include "duckdb/common/types/row_data_collection.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

void RowDataCollectionScanner::ScanState::PinData() {
	auto &rows = scanner.rows;
	D_ASSERT(block_idx < rows.blocks.size());
	auto &data_block = rows.blocks[block_idx];
	if (!data_handle || data_handle->handle->BlockId() != data_block.block->BlockId()) {
		data_handle = rows.buffer_manager.Pin(data_block.block);
	}
	if (scanner.layout.AllConstant()) {
		return;
	}

	auto &heap = scanner.heap;
	D_ASSERT(block_idx < heap.blocks.size());
	auto &heap_block = heap.blocks[block_idx];
	if (!heap_handle || heap_handle->handle->BlockId() != heap_block.block->BlockId()) {
		heap_handle = heap.buffer_manager.Pin(heap_block.block);
	}
}

RowDataCollectionScanner::RowDataCollectionScanner(RowDataCollection &rows_p, RowDataCollection &heap_p,
                                                   const RowLayout &layout_p, bool flush_p)
    : rows(rows_p), heap(heap_p), layout(layout_p), read_state(*this), total_count(rows.count), total_scanned(0),
      flush(flush_p) {
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
		const data_ptr_t data_ptr = read_state.data_handle->Ptr() + read_state.entry_idx * row_width;
		// Set up the next pointers
		data_ptr_t row_ptr = data_ptr;
		for (idx_t i = 0; i < next; i++) {
			data_pointers[scanned + i] = row_ptr;
			row_ptr += row_width;
		}
		// Unswizzle the offsets back to pointers (if needed)
		if (!layout.AllConstant()) {
			RowOperations::UnswizzlePointers(layout, data_ptr, read_state.heap_handle->Ptr(), next);
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

}; // namespace duckdb
