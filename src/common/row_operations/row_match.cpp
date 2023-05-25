//===--------------------------------------------------------------------===//
// row_match.cpp
// Description: This file contains the implementation of the match operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/operator/constant_operators.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"

namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;
using Predicates = RowOperations::Predicates;

template <typename OP>
static idx_t SelectComparison(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                              SelectionVector *true_sel, SelectionVector *false_sel) {
	throw NotImplementedException("Unsupported nested comparison operand for RowOperations::Match");
}

template <>
idx_t SelectComparison<Equals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<NotEquals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedNotEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<GreaterThan>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                    SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<GreaterThanEquals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                          SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThanEquals(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<LessThan>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThan(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<LessThanEquals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                       SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThanEquals(left, right, &sel, count, true_sel, false_sel);
}

template <class T, class OP, bool NO_MATCH_SEL>
static void TemplatedMatchType(UnifiedVectorFormat &col, Vector &rows, SelectionVector &sel, idx_t &count,
                               idx_t col_offset, idx_t col_no, SelectionVector *no_match, idx_t &no_match_count) {
	// Precompute row_mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto data = UnifiedVectorFormat::GetData<T>(col);
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

//! Forward declaration for recursion
template <class OP, bool NO_MATCH_SEL>
static void TemplatedMatchOp(Vector &vec, UnifiedVectorFormat &col, const TupleDataLayout &layout, Vector &rows,
                             SelectionVector &sel, idx_t &count, idx_t col_no, SelectionVector *no_match,
                             idx_t &no_match_count, const idx_t original_count);

template <class OP, bool NO_MATCH_SEL>
static void TemplatedMatchStruct(Vector &vec, UnifiedVectorFormat &col, const TupleDataLayout &layout, Vector &rows,
                                 SelectionVector &sel, idx_t &count, const idx_t col_no, SelectionVector *no_match,
                                 idx_t &no_match_count, const idx_t original_count) {
	// Precompute row_mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	// Work our way through the validity of the whole struct
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
				if (!isnull) {
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

			if (!isnull) {
				sel.set_index(match_count++, idx);
			} else {
				if (NO_MATCH_SEL) {
					no_match->set_index(no_match_count++, idx);
				}
			}
		}
	}
	count = match_count;

	// Now we construct row pointers to the structs
	Vector struct_rows(LogicalTypeId::POINTER);
	auto struct_ptrs = FlatVector::GetData<data_ptr_t>(struct_rows);

	const auto col_offset = layout.GetOffsets()[col_no];
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto row = ptrs[idx];
		struct_ptrs[idx] = row + col_offset;
	}

	// Get the struct layout, child columns, then recurse
	const auto &struct_layout = layout.GetStructLayout(col_no);
	auto &struct_entries = StructVector::GetEntries(vec);
	D_ASSERT(struct_layout.ColumnCount() == struct_entries.size());
	for (idx_t struct_col_no = 0; struct_col_no < struct_layout.ColumnCount(); struct_col_no++) {
		auto &struct_vec = *struct_entries[struct_col_no];
		UnifiedVectorFormat struct_col;
		struct_vec.ToUnifiedFormat(original_count, struct_col);
		TemplatedMatchOp<OP, NO_MATCH_SEL>(struct_vec, struct_col, struct_layout, struct_rows, sel, count,
		                                   struct_col_no, no_match, no_match_count, original_count);
	}
}

template <class OP, bool NO_MATCH_SEL>
static void TemplatedMatchList(Vector &col, Vector &rows, SelectionVector &sel, idx_t &count,
                               const TupleDataLayout &layout, const idx_t col_no, SelectionVector *no_match,
                               idx_t &no_match_count) {
	// Gather a dense Vector containing the column values being matched
	Vector key(col.GetType());
	const auto gather_function = TupleDataCollection::GetGatherFunction(col.GetType());
	gather_function.function(layout, rows, col_no, sel, count, key, *FlatVector::IncrementalSelectionVector(), key,
	                         gather_function.child_functions);

	// Densify the input column
	Vector sliced(col, sel, count);

	if (NO_MATCH_SEL) {
		SelectionVector no_match_sel_offset(no_match->data() + no_match_count);
		auto match_count = SelectComparison<OP>(sliced, key, sel, count, &sel, &no_match_sel_offset);
		no_match_count += count - match_count;
		count = match_count;
	} else {
		count = SelectComparison<OP>(sliced, key, sel, count, &sel, nullptr);
	}
}

template <class OP, bool NO_MATCH_SEL>
static void TemplatedMatchOp(Vector &vec, UnifiedVectorFormat &col, const TupleDataLayout &layout, Vector &rows,
                             SelectionVector &sel, idx_t &count, idx_t col_no, SelectionVector *no_match,
                             idx_t &no_match_count, const idx_t original_count) {
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
	case PhysicalType::STRUCT:
		TemplatedMatchStruct<OP, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match, no_match_count,
		                                       original_count);
		break;
	case PhysicalType::LIST:
		TemplatedMatchList<OP, NO_MATCH_SEL>(vec, rows, sel, count, layout, col_no, no_match, no_match_count);
		break;
	default:
		throw InternalException("Unsupported column type for RowOperations::Match");
	}
}

template <bool NO_MATCH_SEL>
static void TemplatedMatch(DataChunk &columns, UnifiedVectorFormat col_data[], const TupleDataLayout &layout,
                           Vector &rows, const Predicates &predicates, SelectionVector &sel, idx_t &count,
                           SelectionVector *no_match, idx_t &no_match_count) {
	for (idx_t col_no = 0; col_no < predicates.size(); ++col_no) {
		auto &vec = columns.data[col_no];
		auto &col = col_data[col_no];
		switch (predicates[col_no]) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		case ExpressionType::COMPARE_DISTINCT_FROM:
			TemplatedMatchOp<Equals, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match, no_match_count,
			                                       count);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			TemplatedMatchOp<NotEquals, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match,
			                                          no_match_count, count);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			TemplatedMatchOp<GreaterThan, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match,
			                                            no_match_count, count);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			TemplatedMatchOp<GreaterThanEquals, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match,
			                                                  no_match_count, count);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			TemplatedMatchOp<LessThan, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match,
			                                         no_match_count, count);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			TemplatedMatchOp<LessThanEquals, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match,
			                                               no_match_count, count);
			break;
		default:
			throw InternalException("Unsupported comparison type for RowOperations::Match");
		}
	}
}

idx_t RowOperations::Match(DataChunk &columns, UnifiedVectorFormat col_data[], const TupleDataLayout &layout,
                           Vector &rows, const Predicates &predicates, SelectionVector &sel, idx_t count,
                           SelectionVector *no_match, idx_t &no_match_count) {
	if (no_match) {
		TemplatedMatch<true>(columns, col_data, layout, rows, predicates, sel, count, no_match, no_match_count);
	} else {
		TemplatedMatch<false>(columns, col_data, layout, rows, predicates, sel, count, no_match, no_match_count);
	}

	return count;
}

} // namespace duckdb
