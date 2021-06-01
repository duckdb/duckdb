//===--------------------------------------------------------------------===//
// row_scatter.cpp
// Description: This file contains the implementation of the row scattering
//              operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/row_operations/row_operations.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row_layout.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

template <class T>
static void TemplatedScatter(VectorData &gdata, Vector &addresses, const SelectionVector &sel, idx_t count,
                             idx_t col_offset, idx_t col_idx) {
	auto data = (T *)gdata.data;
	auto pointers = FlatVector::GetData<data_ptr_t>(addresses);

	if (!gdata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto pointer_idx = sel.get_index(i);
			auto group_idx = gdata.sel->get_index(pointer_idx);
			auto ptr = pointers[pointer_idx] + col_offset;

			auto isnull = !gdata.validity.RowIsValid(group_idx);
			T store_value = isnull ? NullValue<T>() : data[group_idx];
			Store<T>(store_value, ptr);
			if (isnull) {
				ValidityBytes col_mask(pointers[pointer_idx]);
				col_mask.SetInvalidUnsafe(col_idx);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto pointer_idx = sel.get_index(i);
			auto group_idx = gdata.sel->get_index(pointer_idx);
			auto ptr = pointers[pointer_idx] + col_offset;

			Store<T>(data[group_idx], ptr);
		}
	}
}

void RowOperations::Scatter(VectorData group_data[], const RowLayout &layout, Vector &addresses,
                            StringHeap &string_heap, const SelectionVector &sel, idx_t count) {
	if (count == 0) {
		return;
	}

	// Set the validity mask for each row before inserting data
	auto pointers = FlatVector::GetData<data_ptr_t>(addresses);
	for (idx_t i = 0; i < count; ++i) {
		auto row_idx = sel.get_index(i);
		auto row = pointers[row_idx];
		ValidityBytes(row).SetAllValid(layout.ColumnCount());
	}

	auto &offsets = layout.GetOffsets();
	auto &types = layout.GetTypes();
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		auto &gdata = group_data[col_idx];
		auto col_offset = offsets[col_idx];

		switch (types[col_idx].InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedScatter<int8_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::INT16:
			TemplatedScatter<int16_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::INT32:
			TemplatedScatter<int32_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::INT64:
			TemplatedScatter<int64_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::UINT8:
			TemplatedScatter<uint8_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::UINT16:
			TemplatedScatter<uint16_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::UINT32:
			TemplatedScatter<uint32_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::UINT64:
			TemplatedScatter<uint64_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::INT128:
			TemplatedScatter<hugeint_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::FLOAT:
			TemplatedScatter<float>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::DOUBLE:
			TemplatedScatter<double>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::INTERVAL:
			TemplatedScatter<interval_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::HASH:
			TemplatedScatter<hash_t>(gdata, addresses, sel, count, col_offset, col_idx);
			break;
		case PhysicalType::VARCHAR: {
			auto string_data = (string_t *)gdata.data;
			auto pointers = FlatVector::GetData<data_ptr_t>(addresses);

			for (idx_t i = 0; i < count; i++) {
				auto pointer_idx = sel.get_index(i);
				auto group_idx = gdata.sel->get_index(pointer_idx);
				auto ptr = pointers[pointer_idx] + col_offset;
				if (!gdata.validity.RowIsValid(group_idx)) {
					ValidityBytes col_mask(pointers[pointer_idx]);
					col_mask.SetInvalidUnsafe(col_idx);
					Store<string_t>(NullValue<string_t>(), ptr);
				} else if (string_data[group_idx].IsInlined()) {
					Store<string_t>(string_data[group_idx], ptr);
				} else {
					Store<string_t>(
					    string_heap.AddBlob(string_data[group_idx].GetDataUnsafe(), string_data[group_idx].GetSize()),
					    ptr);
				}
			}
			break;
		}
		default:
			throw Exception("Unsupported type for row scatter");
		}
	}
}

} // namespace duckdb
