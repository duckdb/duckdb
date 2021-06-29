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

void RowOperations::SwizzleColumns(const RowLayout &layout, const data_ptr_t &row_ptr, const idx_t &count) {
	const idx_t row_width = layout.GetRowWidth();
	// Swizzle the blob columns one by one
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		auto physical_type = layout.GetTypes()[col_idx].InternalType();
		if (TypeIsConstantSize(physical_type)) {
			continue;
		}
		data_ptr_t heap_pointer_ptr = row_ptr + layout.GetHeapPointerOffset();
		data_ptr_t col_ptr = row_ptr + layout.GetOffsets()[col_idx];
		if (physical_type == PhysicalType::VARCHAR) {
			// Replace the pointer with the computed offset (if non-inlined)
			for (idx_t i = 0; i < count; i++) {
				const string_t str = Load<string_t>(col_ptr);
				if (!str.IsInlined()) {
					data_ptr_t ptr_ptr = col_ptr + sizeof(uint32_t);
					Store<idx_t>(Load<data_ptr_t>(ptr_ptr) - Load<data_ptr_t>(heap_pointer_ptr), ptr_ptr);
					heap_pointer_ptr += row_width;
					col_ptr += row_width;
				}
			}
		} else {
			// Replace the pointer with the computed offset
			for (idx_t i = 0; i < count; i++) {
				Store<idx_t>(Load<data_ptr_t>(col_ptr) - Load<data_ptr_t>(heap_pointer_ptr), col_ptr);
				heap_pointer_ptr += row_width;
				col_ptr += row_width;
			}
		}
	}
}

void RowOperations::SwizzleHeapPointer(const RowLayout &layout, data_ptr_t row_ptr, const data_ptr_t &heap_base_ptr,
                                       const idx_t &count) {
	row_ptr += layout.GetHeapPointerOffset();
	data_ptr_t heap_ptr = heap_base_ptr;
	for (idx_t i = 0; i < count; i++) {
		Store<idx_t>(heap_ptr - heap_base_ptr, row_ptr);
		heap_ptr += Load<idx_t>(heap_ptr);
	}
}

void RowOperations::Unswizzle(const RowLayout &layout, const data_ptr_t &row_base_ptr, const data_ptr_t &heap_base_ptr,
                              const idx_t &count) {
	const idx_t row_width = layout.GetRowWidth();
	data_ptr_t heap_offset_ptr = row_base_ptr + layout.GetHeapPointerOffset();
	// Compute the base pointer to each row
	vector<data_ptr_t> heap_row_pointers(count);
	for (idx_t i = 0; i < count; i++) {
		heap_row_pointers[i] = heap_base_ptr + Load<idx_t>(heap_offset_ptr);
		heap_offset_ptr += row_width;
	}
	// Unswizzle the columns one by one
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		auto physical_type = layout.GetTypes()[col_idx].InternalType();
		if (TypeIsConstantSize(physical_type)) {
			continue;
		}
		data_ptr_t col_ptr = row_base_ptr + layout.GetOffsets()[col_idx];
		if (physical_type == PhysicalType::VARCHAR) {
			// Replace offset with the pointer (if non-inlined)
			for (idx_t i = 0; i < count; i++) {
				const string_t str = Load<string_t>(col_ptr);
				if (!str.IsInlined()) {
					data_ptr_t offset_ptr = col_ptr + sizeof(uint32_t);
					Store<data_ptr_t>(heap_row_pointers[i] + Load<idx_t>(offset_ptr), offset_ptr);
					col_ptr += row_width;
				}
			}
		} else {
			// Replace the offset with the pointer
			for (idx_t i = 0; i < count; i++) {
				Store<data_ptr_t>(heap_row_pointers[i] + Load<idx_t>(col_ptr), col_ptr);
				col_ptr += row_width;
			}
		}
	}
}

} // namespace duckdb
