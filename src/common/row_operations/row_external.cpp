//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_operations/row_external.cpp
//
//
//===----------------------------------------------------------------------===//
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row/row_layout.hpp"

namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

void RowOperations::SwizzleColumns(const RowLayout &layout, const data_ptr_t base_row_ptr, const idx_t count) {
	const idx_t row_width = layout.GetRowWidth();
	data_ptr_t heap_row_ptrs[STANDARD_VECTOR_SIZE];
	idx_t done = 0;
	while (done != count) {
		const idx_t next = MinValue<idx_t>(count - done, STANDARD_VECTOR_SIZE);
		const data_ptr_t row_ptr = base_row_ptr + done * row_width;
		// Load heap row pointers
		data_ptr_t heap_ptr_ptr = row_ptr + layout.GetHeapOffset();
		for (idx_t i = 0; i < next; i++) {
			heap_row_ptrs[i] = Load<data_ptr_t>(heap_ptr_ptr);
			heap_ptr_ptr += row_width;
		}
		// Loop through the blob columns
		for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
			auto physical_type = layout.GetTypes()[col_idx].InternalType();
			if (TypeIsConstantSize(physical_type)) {
				continue;
			}
			data_ptr_t col_ptr = row_ptr + layout.GetOffsets()[col_idx];
			if (physical_type == PhysicalType::VARCHAR) {
				data_ptr_t string_ptr = col_ptr + string_t::HEADER_SIZE;
				for (idx_t i = 0; i < next; i++) {
					if (Load<uint32_t>(col_ptr) > string_t::INLINE_LENGTH) {
						// Overwrite the string pointer with the within-row offset (if not inlined)
						Store<idx_t>(UnsafeNumericCast<idx_t>(Load<data_ptr_t>(string_ptr) - heap_row_ptrs[i]),
						             string_ptr);
					}
					col_ptr += row_width;
					string_ptr += row_width;
				}
			} else {
				// Non-varchar blob columns
				for (idx_t i = 0; i < next; i++) {
					// Overwrite the column data pointer with the within-row offset
					Store<idx_t>(UnsafeNumericCast<idx_t>(Load<data_ptr_t>(col_ptr) - heap_row_ptrs[i]), col_ptr);
					col_ptr += row_width;
				}
			}
		}
		done += next;
	}
}

void RowOperations::SwizzleHeapPointer(const RowLayout &layout, data_ptr_t row_ptr, const data_ptr_t heap_base_ptr,
                                       const idx_t count, const idx_t base_offset) {
	const idx_t row_width = layout.GetRowWidth();
	row_ptr += layout.GetHeapOffset();
	idx_t cumulative_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		Store<idx_t>(base_offset + cumulative_offset, row_ptr);
		cumulative_offset += Load<uint32_t>(heap_base_ptr + cumulative_offset);
		row_ptr += row_width;
	}
}

void RowOperations::CopyHeapAndSwizzle(const RowLayout &layout, data_ptr_t row_ptr, const data_ptr_t heap_base_ptr,
                                       data_ptr_t heap_ptr, const idx_t count) {
	const auto row_width = layout.GetRowWidth();
	const auto heap_offset = layout.GetHeapOffset();
	for (idx_t i = 0; i < count; i++) {
		// Figure out source and size
		const auto source_heap_ptr = Load<data_ptr_t>(row_ptr + heap_offset);
		const auto size = Load<uint32_t>(source_heap_ptr);
		D_ASSERT(size >= sizeof(uint32_t));

		// Copy and swizzle
		memcpy(heap_ptr, source_heap_ptr, size);
		Store<idx_t>(UnsafeNumericCast<idx_t>(heap_ptr - heap_base_ptr), row_ptr + heap_offset);

		// Increment for next iteration
		row_ptr += row_width;
		heap_ptr += size;
	}
}

void RowOperations::UnswizzleHeapPointer(const RowLayout &layout, const data_ptr_t base_row_ptr,
                                         const data_ptr_t base_heap_ptr, const idx_t count) {
	const auto row_width = layout.GetRowWidth();
	data_ptr_t heap_ptr_ptr = base_row_ptr + layout.GetHeapOffset();
	for (idx_t i = 0; i < count; i++) {
		Store<data_ptr_t>(base_heap_ptr + Load<idx_t>(heap_ptr_ptr), heap_ptr_ptr);
		heap_ptr_ptr += row_width;
	}
}

static inline void VerifyUnswizzledString(const RowLayout &layout, const idx_t &col_idx, const data_ptr_t &row_ptr) {
#ifdef DEBUG
	if (layout.GetTypes()[col_idx].id() != LogicalTypeId::VARCHAR) {
		return;
	}
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	ValidityBytes row_mask(row_ptr);
	if (row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
		auto str = Load<string_t>(row_ptr + layout.GetOffsets()[col_idx]);
		str.Verify();
	}
#endif
}

void RowOperations::UnswizzlePointers(const RowLayout &layout, const data_ptr_t base_row_ptr,
                                      const data_ptr_t base_heap_ptr, const idx_t count) {
	const idx_t row_width = layout.GetRowWidth();
	data_ptr_t heap_row_ptrs[STANDARD_VECTOR_SIZE];
	idx_t done = 0;
	while (done != count) {
		const idx_t next = MinValue<idx_t>(count - done, STANDARD_VECTOR_SIZE);
		const data_ptr_t row_ptr = base_row_ptr + done * row_width;
		// Restore heap row pointers
		data_ptr_t heap_ptr_ptr = row_ptr + layout.GetHeapOffset();
		for (idx_t i = 0; i < next; i++) {
			heap_row_ptrs[i] = base_heap_ptr + Load<idx_t>(heap_ptr_ptr);
			Store<data_ptr_t>(heap_row_ptrs[i], heap_ptr_ptr);
			heap_ptr_ptr += row_width;
		}
		// Loop through the blob columns
		for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
			auto physical_type = layout.GetTypes()[col_idx].InternalType();
			if (TypeIsConstantSize(physical_type)) {
				continue;
			}
			data_ptr_t col_ptr = row_ptr + layout.GetOffsets()[col_idx];
			if (physical_type == PhysicalType::VARCHAR) {
				data_ptr_t string_ptr = col_ptr + string_t::HEADER_SIZE;
				for (idx_t i = 0; i < next; i++) {
					if (Load<uint32_t>(col_ptr) > string_t::INLINE_LENGTH) {
						// Overwrite the string offset with the pointer (if not inlined)
						Store<data_ptr_t>(heap_row_ptrs[i] + Load<idx_t>(string_ptr), string_ptr);
						VerifyUnswizzledString(layout, col_idx, row_ptr + i * row_width);
					}
					col_ptr += row_width;
					string_ptr += row_width;
				}
			} else {
				// Non-varchar blob columns
				for (idx_t i = 0; i < next; i++) {
					// Overwrite the column data offset with the pointer
					Store<data_ptr_t>(heap_row_ptrs[i] + Load<idx_t>(col_ptr), col_ptr);
					col_ptr += row_width;
				}
			}
		}
		done += next;
	}
}

} // namespace duckdb
