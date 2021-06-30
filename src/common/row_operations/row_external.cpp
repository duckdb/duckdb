//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_operations/row_external.cpp
//
//
//===----------------------------------------------------------------------===//
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row_layout.hpp"

namespace duckdb {

void RowOperations::SwizzleColumns(const RowLayout &layout, const data_ptr_t &base_row_ptr, const idx_t &count) {
	const idx_t row_width = layout.GetRowWidth();
	const idx_t heap_pointer_offset = layout.GetHeapPointerOffset();
	// Swizzle the blob columns one by one
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		auto physical_type = layout.GetTypes()[col_idx].InternalType();
		if (TypeIsConstantSize(physical_type)) {
			continue;
		}
		data_ptr_t row_ptr = base_row_ptr;
		const idx_t &col_offset = layout.GetOffsets()[col_idx];
		if (physical_type == PhysicalType::VARCHAR) {
			// Replace the pointer with the computed offset (if non-inlined)
			for (idx_t i = 0; i < count; i++) {
				const string_t str = Load<string_t>(row_ptr + col_offset);
				if (!str.IsInlined()) {
					// Load the pointer to the start of the row in the heap
					data_ptr_t heap_row_ptr = Load<data_ptr_t>(row_ptr + heap_pointer_offset);
					// This is where the pointer that points to the heap is stored in the RowLayout
					data_ptr_t col_ptr = row_ptr + col_offset + sizeof(uint32_t);
					// Load the pointer to the data of this column in the same heap row
					data_ptr_t heap_col_ptr = Load<data_ptr_t>(col_ptr);
					// Overwrite the column data pointer with the within-row offset (pointer arithmetic)
					Store<idx_t>(heap_col_ptr - heap_row_ptr, col_ptr);
				}
				row_ptr += row_width;
			}
		} else {
			// Replace the pointer with the computed offset
			for (idx_t i = 0; i < count; i++) {
				// Load the pointer to the start of the row in the heap
				data_ptr_t heap_row_ptr = Load<data_ptr_t>(row_ptr + heap_pointer_offset);
				// This is where the pointer that points to the heap is stored in the RowLayout
				data_ptr_t col_ptr = row_ptr + col_offset;
				// Load the pointer to the data of this column in the same heap row
				data_ptr_t heap_col_ptr = Load<data_ptr_t>(col_ptr);
				// Overwrite the column data pointer with the within-row offset (pointer arithmetic)
				Store<idx_t>(heap_col_ptr - heap_row_ptr, col_ptr);
				row_ptr += row_width;
			}
		}
	}
}

void RowOperations::SwizzleHeapPointer(const RowLayout &layout, data_ptr_t row_ptr, const data_ptr_t &heap_base_ptr,
                                       const idx_t &count) {
	const idx_t row_width = layout.GetRowWidth();
	const idx_t heap_pointer_offset = layout.GetHeapPointerOffset();
	idx_t cumulative_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		Store<idx_t>(cumulative_offset, row_ptr + heap_pointer_offset);
		cumulative_offset += Load<idx_t>(heap_base_ptr + cumulative_offset);
		row_ptr += row_width;
	}
}

void RowOperations::UnswizzleHeapPointer(const RowLayout &layout, data_ptr_t row_ptr, const data_ptr_t &heap_base_ptr,
                                         const idx_t &count) {
	const idx_t row_width = layout.GetRowWidth();
	const idx_t heap_pointer_offset = layout.GetHeapPointerOffset();
	for (idx_t i = 0; i < count; i++) {
		data_ptr_t heap_pointer_location = row_ptr + heap_pointer_offset;
		idx_t heap_row_offset = Load<idx_t>(heap_pointer_location);
		Store<data_ptr_t>(heap_base_ptr + heap_row_offset, heap_pointer_location);
		row_ptr += row_width;
	}
}

void RowOperations::UnswizzleColumns(const RowLayout &layout, const data_ptr_t &base_row_ptr, const idx_t &count) {
	const idx_t row_width = layout.GetRowWidth();
	const idx_t heap_pointer_offset = layout.GetHeapPointerOffset();
	// Unswizzle the columns one by one
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		auto physical_type = layout.GetTypes()[col_idx].InternalType();
		if (TypeIsConstantSize(physical_type)) {
			continue;
		}
		const idx_t col_offset = layout.GetOffsets()[col_idx];
		data_ptr_t row_ptr = base_row_ptr;
		if (physical_type == PhysicalType::VARCHAR) {
			// Replace offset with the pointer (if non-inlined)
			for (idx_t i = 0; i < count; i++) {
				const string_t str = Load<string_t>(row_ptr + col_offset);
				if (!str.IsInlined()) {
					// Load the pointer to the start of the row in the heap
					data_ptr_t heap_row_ptr = Load<data_ptr_t>(row_ptr + heap_pointer_offset);
					// This is where the pointer that points to the heap is stored in the RowLayout
					data_ptr_t col_ptr = row_ptr + col_offset + sizeof(uint32_t);
					// Load the offset to the data of this column in the same heap row
					idx_t heap_col_offset = Load<idx_t>(col_ptr);
					// Overwrite the column data offset with the pointer
					Store<data_ptr_t>(heap_row_ptr + heap_col_offset, col_ptr);
				}
				row_ptr += row_width;
			}
		} else {
			// Replace the offset with the pointer
			for (idx_t i = 0; i < count; i++) {
				// Load the pointer to the start of the row in the heap
				data_ptr_t heap_row_ptr = Load<data_ptr_t>(row_ptr + heap_pointer_offset);
				// This is where the pointer that points to the heap is stored in the RowLayout
				data_ptr_t col_ptr = row_ptr + col_offset;
				// Load the offset to the data of this column in the same heap row
				idx_t heap_col_offset = Load<idx_t>(col_ptr);
				// Overwrite the column data offset with the pointer
				Store<data_ptr_t>(heap_row_ptr + heap_col_offset, col_ptr);
				row_ptr += row_width;
			}
		}
	}
}

} // namespace duckdb
