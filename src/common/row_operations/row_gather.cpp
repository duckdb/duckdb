//===--------------------------------------------------------------------===//
// row_gather.cpp
// Description: This file contains the implementation of the gather operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/constant_operators.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row/row_data_collection.hpp"
#include "duckdb/common/types/row/row_layout.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/uhugeint.hpp"

namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

template <class T>
static void TemplatedGatherLoop(Vector &rows, const SelectionVector &row_sel, Vector &col,
                                const SelectionVector &col_sel, idx_t count, const RowLayout &layout, idx_t col_no,
                                idx_t build_size) {
	// Precompute mask indexes
	const auto &offsets = layout.GetOffsets();
	const auto col_offset = offsets[col_no];
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	auto data = FlatVector::GetData<T>(col);
	auto &col_mask = FlatVector::Validity(col);

	for (idx_t i = 0; i < count; i++) {
		auto row_idx = row_sel.get_index(i);
		auto row = ptrs[row_idx];
		auto col_idx = col_sel.get_index(i);
		data[col_idx] = Load<T>(row + col_offset);
		ValidityBytes row_mask(row, layout.ColumnCount());
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			if (build_size > STANDARD_VECTOR_SIZE && col_mask.AllValid()) {
				//! We need to initialize the mask with the vector size.
				col_mask.Initialize(build_size);
			}
			col_mask.SetInvalid(col_idx);
		}
	}
}

static void GatherVarchar(Vector &rows, const SelectionVector &row_sel, Vector &col, const SelectionVector &col_sel,
                          idx_t count, const RowLayout &layout, idx_t col_no, idx_t build_size,
                          data_ptr_t base_heap_ptr) {
	// Precompute mask indexes
	const auto &offsets = layout.GetOffsets();
	const auto col_offset = offsets[col_no];
	const auto heap_offset = layout.GetHeapOffset();
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	auto data = FlatVector::GetData<string_t>(col);
	auto &col_mask = FlatVector::Validity(col);

	for (idx_t i = 0; i < count; i++) {
		auto row_idx = row_sel.get_index(i);
		auto row = ptrs[row_idx];
		auto col_idx = col_sel.get_index(i);
		auto col_ptr = row + col_offset;
		data[col_idx] = Load<string_t>(col_ptr);
		ValidityBytes row_mask(row, layout.ColumnCount());
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			if (build_size > STANDARD_VECTOR_SIZE && col_mask.AllValid()) {
				//! We need to initialize the mask with the vector size.
				col_mask.Initialize(build_size);
			}
			col_mask.SetInvalid(col_idx);
		} else if (base_heap_ptr && Load<uint32_t>(col_ptr) > string_t::INLINE_LENGTH) {
			//	Not inline, so unswizzle the copied pointer the pointer
			auto heap_ptr_ptr = row + heap_offset;
			auto heap_row_ptr = base_heap_ptr + Load<idx_t>(heap_ptr_ptr);
			auto string_ptr = data_ptr_t(data + col_idx) + string_t::HEADER_SIZE;
			Store<data_ptr_t>(heap_row_ptr + Load<idx_t>(string_ptr), string_ptr);
#ifdef DEBUG
			data[col_idx].Verify();
#endif
		}
	}
}

static void GatherNestedVector(Vector &rows, const SelectionVector &row_sel, Vector &col,
                               const SelectionVector &col_sel, idx_t count, const RowLayout &layout, idx_t col_no,
                               data_ptr_t base_heap_ptr) {
	const auto &offsets = layout.GetOffsets();
	const auto col_offset = offsets[col_no];
	const auto heap_offset = layout.GetHeapOffset();
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	// Build the gather locations
	auto data_locations = make_unsafe_uniq_array_uninitialized<data_ptr_t>(count);
	auto mask_locations = make_unsafe_uniq_array_uninitialized<data_ptr_t>(count);
	for (idx_t i = 0; i < count; i++) {
		auto row_idx = row_sel.get_index(i);
		auto row = ptrs[row_idx];
		mask_locations[i] = row;
		auto col_ptr = ptrs[row_idx] + col_offset;
		if (base_heap_ptr) {
			auto heap_ptr_ptr = row + heap_offset;
			auto heap_row_ptr = base_heap_ptr + Load<idx_t>(heap_ptr_ptr);
			data_locations[i] = heap_row_ptr + Load<idx_t>(col_ptr);
		} else {
			data_locations[i] = Load<data_ptr_t>(col_ptr);
		}
	}

	// Deserialise into the selected locations
	NestedValidity parent_validity(mask_locations.get(), col_no);
	RowOperations::HeapGather(col, count, col_sel, data_locations.get(), &parent_validity);
}

void RowOperations::Gather(Vector &rows, const SelectionVector &row_sel, Vector &col, const SelectionVector &col_sel,
                           const idx_t count, const RowLayout &layout, const idx_t col_no, const idx_t build_size,
                           data_ptr_t heap_ptr) {
	D_ASSERT(rows.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(rows.GetType().id() == LogicalTypeId::POINTER); // "Cannot gather from non-pointer type!"

	col.SetVectorType(VectorType::FLAT_VECTOR);
	switch (col.GetType().InternalType()) {
	case PhysicalType::UINT8:
		TemplatedGatherLoop<uint8_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::UINT16:
		TemplatedGatherLoop<uint16_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::UINT32:
		TemplatedGatherLoop<uint32_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::UINT64:
		TemplatedGatherLoop<uint64_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::UINT128:
		TemplatedGatherLoop<uhugeint_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedGatherLoop<int8_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::INT16:
		TemplatedGatherLoop<int16_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::INT32:
		TemplatedGatherLoop<int32_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::INT64:
		TemplatedGatherLoop<int64_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::INT128:
		TemplatedGatherLoop<hugeint_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::FLOAT:
		TemplatedGatherLoop<float>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGatherLoop<double>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::INTERVAL:
		TemplatedGatherLoop<interval_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::VARCHAR:
		GatherVarchar(rows, row_sel, col, col_sel, count, layout, col_no, build_size, heap_ptr);
		break;
	case PhysicalType::LIST:
	case PhysicalType::STRUCT:
	case PhysicalType::ARRAY:
		GatherNestedVector(rows, row_sel, col, col_sel, count, layout, col_no, heap_ptr);
		break;
	default:
		throw InternalException("Unimplemented type for RowOperations::Gather");
	}
}

template <class T>
static void TemplatedFullScanLoop(Vector &rows, Vector &col, idx_t count, idx_t col_offset, idx_t col_no,
                                  idx_t column_count) {
	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	auto data = FlatVector::GetData<T>(col);
	//	auto &col_mask = FlatVector::Validity(col);

	for (idx_t i = 0; i < count; i++) {
		auto row = ptrs[i];
		data[i] = Load<T>(row + col_offset);
		ValidityBytes row_mask(row, column_count);
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			throw InternalException("Null value comparisons not implemented for perfect hash table yet");
			//			col_mask.SetInvalid(i);
		}
	}
}

void RowOperations::FullScanColumn(const TupleDataLayout &layout, Vector &rows, Vector &col, idx_t count,
                                   idx_t col_no) {
	const auto col_offset = layout.GetOffsets()[col_no];
	col.SetVectorType(VectorType::FLAT_VECTOR);
	switch (col.GetType().InternalType()) {
	case PhysicalType::UINT8:
		TemplatedFullScanLoop<uint8_t>(rows, col, count, col_offset, col_no, layout.ColumnCount());
		break;
	case PhysicalType::UINT16:
		TemplatedFullScanLoop<uint16_t>(rows, col, count, col_offset, col_no, layout.ColumnCount());
		break;
	case PhysicalType::UINT32:
		TemplatedFullScanLoop<uint32_t>(rows, col, count, col_offset, col_no, layout.ColumnCount());
		break;
	case PhysicalType::UINT64:
		TemplatedFullScanLoop<uint64_t>(rows, col, count, col_offset, col_no, layout.ColumnCount());
		break;
	case PhysicalType::INT8:
		TemplatedFullScanLoop<int8_t>(rows, col, count, col_offset, col_no, layout.ColumnCount());
		break;
	case PhysicalType::INT16:
		TemplatedFullScanLoop<int16_t>(rows, col, count, col_offset, col_no, layout.ColumnCount());
		break;
	case PhysicalType::INT32:
		TemplatedFullScanLoop<int32_t>(rows, col, count, col_offset, col_no, layout.ColumnCount());
		break;
	case PhysicalType::INT64:
		TemplatedFullScanLoop<int64_t>(rows, col, count, col_offset, col_no, layout.ColumnCount());
		break;
	default:
		throw NotImplementedException("Unimplemented type for RowOperations::FullScanColumn");
	}
}

} // namespace duckdb
