//===--------------------------------------------------------------------===//
// row_scatter.cpp
// Description: This file contains the implementation of the row scattering
//              operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/row_operations/row_operations.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row_data_collection.hpp"
#include "duckdb/common/types/row_layout.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

template <class T>
static void TemplatedScatter(VectorData &col, Vector &rows, const SelectionVector &sel, const idx_t count,
                             const idx_t col_offset, const idx_t col_no) {
	auto data = (T *)col.data;
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	if (!col.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto col_idx = col.sel->get_index(idx);
			auto row = ptrs[idx];

			auto isnull = !col.validity.RowIsValid(col_idx);
			T store_value = isnull ? NullValue<T>() : data[col_idx];
			Store<T>(store_value, row + col_offset);
			if (isnull) {
				ValidityBytes col_mask(ptrs[idx]);
				col_mask.SetInvalidUnsafe(col_no);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto col_idx = col.sel->get_index(idx);
			auto row = ptrs[idx];

			Store<T>(data[col_idx], row + col_offset);
		}
	}
}

static void ComputeStringEntrySizes(const VectorData &col, idx_t entry_sizes[], const SelectionVector &sel,
                                    const idx_t count, const idx_t offset = 0) {
	auto data = (const string_t *)col.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto col_idx = col.sel->get_index(idx) + offset;
		const auto &str = data[col_idx];
		if (col.validity.RowIsValid(col_idx) && !str.IsInlined()) {
			entry_sizes[i] += str.GetSize();
		}
	}
}
static void ScatterStrings(VectorData &col, Vector &rows, RowDataCollection &string_heap, const SelectionVector &sel,
                           const idx_t count, const idx_t col_offset, const idx_t col_no) {

	idx_t entry_sizes[STANDARD_VECTOR_SIZE];
	std::fill_n(entry_sizes, count, 0);
	ComputeStringEntrySizes(col, entry_sizes, sel, count);

	data_ptr_t str_locations[STANDARD_VECTOR_SIZE];
	string_heap.Build(count, str_locations, entry_sizes);

	auto string_data = (string_t *)col.data;
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto col_idx = col.sel->get_index(idx);
		auto row = ptrs[idx];
		if (!col.validity.RowIsValid(col_idx)) {
			ValidityBytes col_mask(row);
			col_mask.SetInvalidUnsafe(col_no);
			Store<string_t>(NullValue<string_t>(), row + col_offset);
		} else if (string_data[col_idx].IsInlined()) {
			Store<string_t>(string_data[col_idx], row + col_offset);
		} else {
			const auto &str = string_data[col_idx];
			string_t inserted((const char *)str_locations[i], str.GetSize());
			memcpy(inserted.GetDataWriteable(), str.GetDataUnsafe(), str.GetSize());
			inserted.Finalize();
			Store<string_t>(inserted, row + col_offset);
		}
	}
}
void RowOperations::Scatter(VectorData group_data[], const RowLayout &layout, Vector &rows,
                            RowDataCollection &string_heap, const SelectionVector &sel, idx_t count) {
	if (count == 0) {
		return;
	}

	// Set the validity mask for each row before inserting data
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	for (idx_t i = 0; i < count; ++i) {
		auto row_idx = sel.get_index(i);
		auto row = ptrs[row_idx];
		ValidityBytes(row).SetAllValid(layout.ColumnCount());
	}

	auto &offsets = layout.GetOffsets();
	auto &types = layout.GetTypes();
	for (idx_t col_no = 0; col_no < types.size(); col_no++) {
		auto &col = group_data[col_no];
		auto col_offset = offsets[col_no];

		switch (types[col_no].InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedScatter<int8_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT16:
			TemplatedScatter<int16_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT32:
			TemplatedScatter<int32_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT64:
			TemplatedScatter<int64_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT8:
			TemplatedScatter<uint8_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT16:
			TemplatedScatter<uint16_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT32:
			TemplatedScatter<uint32_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT64:
			TemplatedScatter<uint64_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT128:
			TemplatedScatter<hugeint_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::FLOAT:
			TemplatedScatter<float>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::DOUBLE:
			TemplatedScatter<double>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INTERVAL:
			TemplatedScatter<interval_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::HASH:
			TemplatedScatter<hash_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::VARCHAR:
			ScatterStrings(col, rows, string_heap, sel, count, col_offset, col_no);
			break;
		default:
			throw Exception("Unsupported type for row scatter");
		}
	}
}

} // namespace duckdb
