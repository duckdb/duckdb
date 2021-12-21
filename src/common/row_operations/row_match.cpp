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
using Predicates = RowOperations::Predicates;

template <typename OP>
static idx_t SelectComparison(Vector &left, Vector &right, idx_t vcount, const SelectionVector &sel, idx_t count,
                              SelectionVector *true_sel, SelectionVector *false_sel) {
	throw NotImplementedException("Unsupported nested comparison operand for RowOperations::Match");
}

template <>
idx_t SelectComparison<Equals>(Vector &left, Vector &right, idx_t vcount, const SelectionVector &sel, idx_t count,
                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedEquals(left, right, vcount, sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<NotEquals>(Vector &left, Vector &right, idx_t vcount, const SelectionVector &sel, idx_t count,
                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedNotEquals(left, right, vcount, sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<GreaterThan>(Vector &left, Vector &right, idx_t vcount, const SelectionVector &sel, idx_t count,
                                    SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedGreaterThan(left, right, vcount, sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<GreaterThanEquals>(Vector &left, Vector &right, idx_t vcount, const SelectionVector &sel,
                                          idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedGreaterThanEquals(left, right, vcount, sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<LessThan>(Vector &left, Vector &right, idx_t vcount, const SelectionVector &sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedLessThan(left, right, vcount, sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<LessThanEquals>(Vector &left, Vector &right, idx_t vcount, const SelectionVector &sel,
                                       idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedLessThanEquals(left, right, vcount, sel, count, true_sel, false_sel);
}

template <class T, class OP, bool NO_MATCH_SEL>
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
					if (NO_MATCH_SEL) {
						no_match->set_index(no_match_count++, idx);
					}
				}
			} else {
				auto value = Load<T>(row + col_offset);
				if (!isnull && OP::template Operation<T>(data[col_idx], value)) {
					sel.set_index(match_count++, idx);
				} else {
					if (NO_MATCH_SEL) {
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
				if (NO_MATCH_SEL) {
					no_match->set_index(no_match_count++, idx);
				}
			}
		}
	}
	count = match_count;
}

template <class OP, bool NO_MATCH_SEL>
static void TemplatedMatchNested(Vector &col, Vector &rows, const idx_t vcount, SelectionVector &sel, idx_t &count,
                                 const idx_t col_offset, const idx_t col_no, SelectionVector *no_match,
                                 idx_t &no_match_count) {
	// Gather a scattered Vector containing the column values being matched
	Vector key(col.GetType());
	RowOperations::Gather(rows, sel, key, sel, count, col_offset, col_no);

	if (NO_MATCH_SEL) {
		SelectionVector no_match_sel_offset(no_match->data() + no_match_count);
		auto match_count = SelectComparison<OP>(col, key, vcount, sel, count, &sel, &no_match_sel_offset);
		no_match_count += count - match_count;
		count = match_count;
	} else {
		count = SelectComparison<OP>(col, key, vcount, sel, count, &sel, nullptr);
	}
}

template <class OP, bool NO_MATCH_SEL>
static void TemplatedMatchOp(Vector &vec, VectorData &col, const idx_t vcount, const RowLayout &layout, Vector &rows,
                             SelectionVector &sel, idx_t &count, idx_t col_no, SelectionVector *no_match,
                             idx_t &no_match_count) {
	if (count == 0) {
		return;
	}
	auto col_offset = layout.GetOffsets()[col_no];
	switch (layout.GetTypes()[col_no].InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedMatchType<int8_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                             no_match_count);
		break;
	case PhysicalType::INT16:
		TemplatedMatchType<int16_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                              no_match_count);
		break;
	case PhysicalType::INT32:
		TemplatedMatchType<int32_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                              no_match_count);
		break;
	case PhysicalType::INT64:
		TemplatedMatchType<int64_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                              no_match_count);
		break;
	case PhysicalType::UINT8:
		TemplatedMatchType<uint8_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                              no_match_count);
		break;
	case PhysicalType::UINT16:
		TemplatedMatchType<uint16_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                               no_match_count);
		break;
	case PhysicalType::UINT32:
		TemplatedMatchType<uint32_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                               no_match_count);
		break;
	case PhysicalType::UINT64:
		TemplatedMatchType<uint64_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                               no_match_count);
		break;
	case PhysicalType::INT128:
		TemplatedMatchType<hugeint_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                                no_match_count);
		break;
	case PhysicalType::FLOAT:
		TemplatedMatchType<float, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                            no_match_count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedMatchType<double, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                             no_match_count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedMatchType<interval_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                                 no_match_count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedMatchType<string_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                               no_match_count);
		break;
	case PhysicalType::LIST:
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
		TemplatedMatchNested<OP, NO_MATCH_SEL>(vec, rows, vcount, sel, count, col_offset, col_no, no_match,
		                                       no_match_count);
		break;
	default:
		throw InternalException("Unsupported column type for RowOperations::Match");
	}
}

template <bool NO_MATCH_SEL>
static void TemplatedMatch(DataChunk &columns, VectorData col_data[], const RowLayout &layout, Vector &rows,
                           const Predicates &predicates, SelectionVector &sel, idx_t &count, SelectionVector *no_match,
                           idx_t &no_match_count) {
	const idx_t vcount = columns.size();
	for (idx_t col_no = 0; col_no < predicates.size(); ++col_no) {
		auto &vec = columns.data[col_no];
		auto &col = col_data[col_no];
		switch (predicates[col_no]) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			TemplatedMatchOp<Equals, NO_MATCH_SEL>(vec, col, vcount, layout, rows, sel, count, col_no, no_match,
			                                       no_match_count);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			TemplatedMatchOp<NotEquals, NO_MATCH_SEL>(vec, col, vcount, layout, rows, sel, count, col_no, no_match,
			                                          no_match_count);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			TemplatedMatchOp<GreaterThan, NO_MATCH_SEL>(vec, col, vcount, layout, rows, sel, count, col_no, no_match,
			                                            no_match_count);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			TemplatedMatchOp<GreaterThanEquals, NO_MATCH_SEL>(vec, col, vcount, layout, rows, sel, count, col_no,
			                                                  no_match, no_match_count);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			TemplatedMatchOp<LessThan, NO_MATCH_SEL>(vec, col, vcount, layout, rows, sel, count, col_no, no_match,
			                                         no_match_count);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			TemplatedMatchOp<LessThanEquals, NO_MATCH_SEL>(vec, col, vcount, layout, rows, sel, count, col_no, no_match,
			                                               no_match_count);
			break;
		default:
			throw InternalException("Unsupported comparison type for RowOperations::Match");
		}
	}
}

idx_t RowOperations::Match(DataChunk &columns, VectorData col_data[], const RowLayout &layout, Vector &rows,
                           const Predicates &predicates, SelectionVector &sel, idx_t count, SelectionVector *no_match,
                           idx_t &no_match_count) {
	if (no_match) {
		TemplatedMatch<true>(columns, col_data, layout, rows, predicates, sel, count, no_match, no_match_count);
	} else {
		TemplatedMatch<false>(columns, col_data, layout, rows, predicates, sel, count, no_match, no_match_count);
	}

	return count;
}

template <class T, class OP>
static void TemplatedMatchRowsType(Vector &rows_left, const SelectionVector &left_sel, Vector &rows_right,
                                   const SelectionVector &right_sel, idx_t rows_count, idx_t col_offset,
                                   idx_t &matches) {
	// Precompute row_mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_offset, entry_idx, idx_in_entry);

	auto left_ptrs = FlatVector::GetData<data_ptr_t>(rows_left);
	auto right_ptrs = FlatVector::GetData<data_ptr_t>(rows_right);
	idx_t match_count = 0;

	for (idx_t i = 0; i < rows_count; i++) {
		auto left_idx = left_sel.get_index(i);
		auto right_idx = right_sel.get_index(i);

		auto left_row = left_ptrs[left_idx];
		auto right_row = right_ptrs[right_idx];
		ValidityBytes left_row_mask(left_row);
		ValidityBytes right_row_mask(right_row);
		auto isnull = !left_row_mask.RowIsValid(left_row_mask.GetValidityEntry(entry_idx), idx_in_entry);

		auto left_value = Load<T>(left_row + col_offset);
		auto right_value = Load<T>(right_row + col_offset);
		if (!isnull && OP::template Operation<T>(left_value, right_value)) {
			match_count++;
		}
	}

	matches = match_count;
}

template <class OP>
static void TemplatedMatchRowsOp(Vector &rows_left, const SelectionVector &left_sel, const RowLayout &layout,
                                 Vector &rows_right, const SelectionVector &right_sel, idx_t rows_count,
                                 idx_t &matches) {
	if (rows_count == 0) {
		return;
	}
	// check whether all the keys are equals
	auto col_offsets = layout.GetOffsets();
	auto condition_types = layout.GetTypes();

	for (idx_t key_idx = 0; key_idx != condition_types.size(); ++key_idx) {
		auto key_type = condition_types[key_idx];
		auto key_offset = col_offsets[key_idx];
		switch (key_type.InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedMatchRowsType<int8_t, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                   matches);
			break;
		case PhysicalType::INT16:
			TemplatedMatchRowsType<int16_t, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                    matches);
			break;
		case PhysicalType::INT32:
			TemplatedMatchRowsType<int32_t, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                    matches);
			break;
		case PhysicalType::INT64:
			TemplatedMatchRowsType<int64_t, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                    matches);
			break;
		case PhysicalType::UINT8:
			TemplatedMatchRowsType<uint8_t, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                    matches);
			break;
		case PhysicalType::UINT16:
			TemplatedMatchRowsType<uint16_t, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                     matches);
			break;
		case PhysicalType::UINT32:
			TemplatedMatchRowsType<uint32_t, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                     matches);
			break;
		case PhysicalType::UINT64:
			TemplatedMatchRowsType<uint64_t, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                     matches);
			break;
		case PhysicalType::INT128:
			TemplatedMatchRowsType<hugeint_t, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                      matches);
			break;
		case PhysicalType::FLOAT:
			TemplatedMatchRowsType<float, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                  matches);
			break;
		case PhysicalType::DOUBLE:
			TemplatedMatchRowsType<double, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                   matches);
			break;
		case PhysicalType::INTERVAL:
			TemplatedMatchRowsType<interval_t, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                       matches);
			break;
		case PhysicalType::VARCHAR:
			TemplatedMatchRowsType<string_t, OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset,
			                                     matches);
			break;
		case PhysicalType::LIST:
		case PhysicalType::MAP:
		case PhysicalType::STRUCT:
			// TemplatedMatchRowsNested<OP>(rows_left, left_sel, rows_right, right_sel, rows_count, key_offset);
			break;
		default:
			throw InternalException("Unsupported column type for RowOperations::Match");
		}
	}
}

idx_t RowOperations::MatchRows(Vector &rows_left, const SelectionVector &left_sel, const RowLayout &layout,
                               Vector &rows_right, const SelectionVector &right_sel, idx_t rows_count) {
	idx_t match_count = 0;
	TemplatedMatchRowsOp<Equals>(rows_left, left_sel, layout, rows_right, right_sel, rows_count, match_count);
	return match_count > 0;
}

} // namespace duckdb
