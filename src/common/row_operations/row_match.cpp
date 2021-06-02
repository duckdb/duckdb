//===--------------------------------------------------------------------===//
// row_match.cpp
// Description: This file contains the implementation of the match operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/constant_operators.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row_layout.hpp"

namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

template <class T, class OP>
static void TemplatedMatchType(VectorData &col, Vector &rows, SelectionVector &sel, idx_t &count, idx_t col_offset,
                               idx_t col_no, SelectionVector *no_match, idx_t &no_match_count) {
	// Precompute row_mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto data = (T *)col.data;
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	idx_t match_count = 0;
	if (!col.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);

			auto row = ptrs[idx];
			ValidityBytes row_mask(row);
			auto isnull = !row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry);

			auto col_idx = col.sel->get_index(idx);
			if (!col.validity.RowIsValid(col_idx)) {
				if (isnull) {
					// match: move to next value to compare
					sel.set_index(match_count++, idx);
				} else {
					if (no_match) {
						no_match->set_index(no_match_count++, idx);
					}
				}
			} else {
				auto value = Load<T>(row + col_offset);
				if (!isnull && OP::template Operation<T>(data[col_idx], value)) {
					sel.set_index(match_count++, idx);
				} else {
					if (no_match) {
						no_match->set_index(no_match_count++, idx);
					}
				}
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);

			auto row = ptrs[idx];
			ValidityBytes row_mask(row);
			auto isnull = !row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry);

			auto col_idx = col.sel->get_index(idx);
			auto value = Load<T>(row + col_offset);
			if (!isnull && OP::template Operation<T>(data[col_idx], value)) {
				sel.set_index(match_count++, idx);
			} else {
				if (no_match) {
					no_match->set_index(no_match_count++, idx);
				}
			}
		}
	}
	count = match_count;
}

template <class OP>
static void TemplatedMatch(const RowLayout &layout, Vector &rows, VectorData col_data[], SelectionVector &sel,
                           idx_t count, SelectionVector *no_match, idx_t &no_match_count) {
	if (count == 0) {
		return;
	}
	auto &offsets = layout.GetOffsets();
	auto &types = layout.GetTypes();
	for (idx_t i = 0; i < types.size(); ++i) {
		auto col_offset = offsets[i];
		auto &col = col_data[i];
		switch (types[i].InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedMatchType<int8_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::INT16:
			TemplatedMatchType<int16_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::INT32:
			TemplatedMatchType<int32_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::INT64:
			TemplatedMatchType<int64_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::UINT8:
			TemplatedMatchType<uint8_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::UINT16:
			TemplatedMatchType<uint16_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::UINT32:
			TemplatedMatchType<uint32_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::UINT64:
			TemplatedMatchType<uint64_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::INT128:
			TemplatedMatchType<hugeint_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::FLOAT:
			TemplatedMatchType<float, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedMatchType<double, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedMatchType<interval_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::HASH:
			TemplatedMatchType<hash_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		case PhysicalType::VARCHAR:
			TemplatedMatchType<string_t, OP>(col, rows, sel, count, col_offset, i, no_match, no_match_count);
			break;
		default:
			throw Exception("Unsupported type for RowOperations::Match");
		}
	}
}

void RowOperations::Match(const RowLayout &layout, Vector &rows, VectorData col_data[], SelectionVector &sel,
                          idx_t count, SelectionVector *no_match, idx_t &no_match_count) {
	TemplatedMatch<Equals>(layout, rows, col_data, sel, count, no_match, no_match_count);
}

} // namespace duckdb
