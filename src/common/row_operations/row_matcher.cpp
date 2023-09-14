#include "duckdb/common/row_operations/row_matcher.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

template <class OP>
struct RowMatchOperator {
	static constexpr bool COMPARE_NULL = false;

	template <class T>
	static bool Operation(const T &left, const T &right, bool left_null, bool right_null) {
		return OP::template Operation<T>(left, right);
	}
};

template <>
struct RowMatchOperator<DistinctFrom> {
	static constexpr bool COMPARE_NULL = true;

	template <class T>
	static bool Operation(const T &left, const T &right, bool left_null, bool right_null) {
		return DistinctFrom::template Operation<T>(left, right, left_null, right_null);
	}
};

template <>
struct RowMatchOperator<NotDistinctFrom> {
	static constexpr bool COMPARE_NULL = true;

	template <class T>
	static bool Operation(const T &left, const T &right, bool left_null, bool right_null) {
		return NotDistinctFrom::template Operation<T>(left, right, left_null, right_null);
	}
};

template <bool NO_MATCH_SEL, class T, class OP>
static idx_t TemplatedMatch(Vector &lhs_vector, const TupleDataVectorFormat &lhs_format, SelectionVector &sel,
                            const idx_t count, const TupleDataLayout &rhs_layout, Vector &rhs_row_locations,
                            const idx_t col_idx, const vector<MatchFunction> &child_functions,
                            SelectionVector *no_match_sel, idx_t &no_match_count) {
	using MATCH_OP = RowMatchOperator<OP>;

	// LHS
	const auto &lhs_sel = *lhs_format.data.sel;
	const auto lhs_data = UnifiedVectorFormat::GetData<T>(lhs_format.data);
	const auto &lhs_validity = lhs_format.data.validity;

	// RHS
	const auto rhs_locations = FlatVector::GetData<data_ptr_t>(rhs_row_locations);
	const auto rhs_offset_in_row = rhs_layout.GetOffsets()[col_idx];
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	idx_t match_count = 0;
	if (lhs_validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			const auto idx = sel.get_index(i);

			const auto &rhs_location = rhs_locations[idx];
			const ValidityBytes rhs_mask(rhs_location);
			const auto rhs_null = !rhs_mask.RowIsValid(rhs_mask.GetValidityEntry(entry_idx), idx_in_entry);

			if (!rhs_null && MATCH_OP::template Operation<T>(lhs_data[lhs_sel.get_index(idx)],
			                                                 Load<T>(rhs_location + rhs_offset_in_row), false, false)) {
				sel.set_index(match_count++, idx);
			} else if (NO_MATCH_SEL) {
				no_match_sel->set_index(no_match_count++, idx);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			const auto idx = sel.get_index(i);

			const auto lhs_idx = lhs_sel.get_index(idx);
			const auto lhs_null = !lhs_validity.RowIsValid(lhs_idx);

			const auto &rhs_location = rhs_locations[idx];
			const ValidityBytes rhs_mask(rhs_location);
			const auto rhs_null = !rhs_mask.RowIsValid(rhs_mask.GetValidityEntry(entry_idx), idx_in_entry);

			if (MATCH_OP::COMPARE_NULL && (lhs_null || rhs_null) &&
			    MATCH_OP::template Operation<T>(lhs_data[lhs_idx], Load<T>(rhs_location + rhs_offset_in_row), lhs_null,
			                                    rhs_null)) {
				sel.set_index(match_count++, idx);
			} else if (NO_MATCH_SEL) {
				no_match_sel->set_index(no_match_count++, idx);
			}
		}
	}
	return match_count;
}

template <bool NO_MATCH_SEL, class OP, class NESTED_OP>
static idx_t StructMatch(Vector &lhs_vector, const TupleDataVectorFormat &lhs_format, SelectionVector &sel,
                         const idx_t count, const TupleDataLayout &rhs_layout, Vector &rhs_row_locations,
                         const idx_t col_idx, const vector<MatchFunction> &child_functions,
                         SelectionVector *no_match_sel, idx_t &no_match_count) {
	using MATCH_OP = RowMatchOperator<OP>;

	// LHS
	const auto &lhs_sel = *lhs_format.data.sel;
	const auto &lhs_validity = lhs_format.data.validity;

	// RHS
	const auto rhs_locations = FlatVector::GetData<data_ptr_t>(rhs_row_locations);
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	idx_t match_count = 0;
	if (!lhs_validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			const auto idx = sel.get_index(i);

			const auto lhs_idx = lhs_sel.get_index(idx);
			const auto lhs_null = !lhs_validity.RowIsValid(lhs_idx);

			const auto &rhs_location = rhs_locations[idx];
			const ValidityBytes rhs_mask(rhs_location);
			const auto rhs_null = !rhs_mask.RowIsValid(rhs_mask.GetValidityEntry(entry_idx), idx_in_entry);

			if (MATCH_OP::COMPARE_NULL && (lhs_null || rhs_null) &&
			    MATCH_OP::template Operation<int32_t>(0, 0, lhs_null, rhs_null)) {
				sel.set_index(match_count++, idx);
			} else if (NO_MATCH_SEL) {
				no_match_sel->set_index(no_match_count++, idx);
			}
		}
	}

	// Create a Vector of pointers to the start of the TupleDataLayout of the STRUCT
	Vector rhs_struct_row_locations(LogicalType::POINTER);
	const auto rhs_offset_in_row = rhs_layout.GetOffsets()[col_idx];
	auto rhs_struct_locations = FlatVector::GetData<data_ptr_t>(rhs_struct_row_locations);
	for (idx_t i = 0; i < count; i++) {
		const auto idx = sel.get_index(i);
		rhs_struct_locations[idx] = rhs_locations[idx] + rhs_offset_in_row;
	}

	// Get the struct layout and struct entries
	const auto &rhs_struct_layout = rhs_layout.GetStructLayout(col_idx);
	auto &lhs_struct_vectors = StructVector::GetEntries(lhs_vector);
	D_ASSERT(rhs_struct_layout.ColumnCount() == lhs_struct_vectors.size());

	for (idx_t struct_col_idx = 0; struct_col_idx < rhs_struct_layout.ColumnCount(); struct_col_idx++) {
		auto &lhs_struct_vector = *lhs_struct_vectors[struct_col_idx];
		auto &lhs_struct_format = lhs_format.child_formats[struct_col_idx];
		const auto &child_function = child_functions[struct_col_idx];
		match_count = child_function.function(lhs_struct_vector, lhs_struct_format, sel, count, rhs_struct_layout,
		                                      rhs_struct_row_locations, struct_col_idx, child_function.child_functions,
		                                      no_match_sel, no_match_count);
	}

	return match_count;
}

template <typename OP>
static idx_t SelectComparison(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                              SelectionVector *true_sel, SelectionVector *false_sel) {
	throw NotImplementedException("Unsupported list comparison operand for RowMatcher::GetMatchFunction");
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

template <bool NO_MATCH_SEL, class OP>
static idx_t ListMatch(Vector &lhs_vector, const TupleDataVectorFormat &lhs_format, SelectionVector &sel,
                       const idx_t count, const TupleDataLayout &rhs_layout, Vector &rhs_row_locations,
                       const idx_t col_idx, const vector<MatchFunction> &child_functions, SelectionVector *no_match_sel,
                       idx_t &no_match_count) {
	const auto &type = rhs_layout.GetTypes()[col_idx];

	// Gather a dense Vector containing the column values being matched
	Vector key(type);
	const auto gather_function = TupleDataCollection::GetGatherFunction(type);
	gather_function.function(rhs_layout, rhs_row_locations, col_idx, sel, count, key,
	                         *FlatVector::IncrementalSelectionVector(), key, gather_function.child_functions);

	// Densify the input column
	Vector sliced(lhs_vector, sel, count);

	if (NO_MATCH_SEL) {
		SelectionVector no_match_sel_offset(no_match_sel->data() + no_match_count);
		auto match_count = SelectComparison<OP>(sliced, key, sel, count, &sel, &no_match_sel_offset);
		no_match_count += count - match_count;
		return match_count;
	} else {
		return SelectComparison<OP>(sliced, key, sel, count, &sel, nullptr);
	}
}

void RowMatcher::Initialize(const bool no_match_sel, const TupleDataLayout &layout, const Predicates &predicates) {
	match_functions.reserve(predicates.size());
	for (idx_t col_idx = 0; col_idx < match_functions.size(); col_idx++) {
		match_functions.push_back(GetMatchFunction(no_match_sel, layout.GetTypes()[col_idx], predicates[col_idx]));
	}
}

idx_t RowMatcher::Match(DataChunk &lhs_vector, const TupleDataVectorFormat &lhs_format, SelectionVector &sel,
                        idx_t count, const TupleDataLayout &rhs_layout, Vector &rhs_row_locations,
                        SelectionVector *no_match_sel, idx_t &no_match_count) {
	for (idx_t col_idx = 0; col_idx < match_functions.size(); col_idx++) {
		const auto &match_function = match_functions[col_idx];
		count = match_function.function(lhs_vector.data[col_idx], lhs_format, sel, count, rhs_layout, rhs_row_locations,
		                                col_idx, match_function.child_functions, no_match_sel, no_match_count);
	}
	return count;
}

MatchFunction RowMatcher::GetMatchFunction(const bool no_match_sel, const LogicalType &type,
                                           const ExpressionType predicate) {
	if (no_match_sel) {
		return GetMatchFunction<true>(type, predicate);
	} else {
		return GetMatchFunction<false>(type, predicate);
	}
}

template <bool NO_MATCH_SEL>
MatchFunction RowMatcher::GetMatchFunction(const LogicalType &type, const ExpressionType predicate) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return GetMatchFunction<NO_MATCH_SEL, bool>(predicate);
	case PhysicalType::INT8:
		return GetMatchFunction<NO_MATCH_SEL, int8_t>(predicate);
	case PhysicalType::INT16:
		return GetMatchFunction<NO_MATCH_SEL, int8_t>(predicate);
	case PhysicalType::INT32:
		return GetMatchFunction<NO_MATCH_SEL, int8_t>(predicate);
	case PhysicalType::INT64:
		return GetMatchFunction<NO_MATCH_SEL, int8_t>(predicate);
	case PhysicalType::INT128:
		return GetMatchFunction<NO_MATCH_SEL, hugeint_t>(predicate);
	case PhysicalType::UINT8:
		return GetMatchFunction<NO_MATCH_SEL, uint8_t>(predicate);
	case PhysicalType::UINT16:
		return GetMatchFunction<NO_MATCH_SEL, uint8_t>(predicate);
	case PhysicalType::UINT32:
		return GetMatchFunction<NO_MATCH_SEL, uint8_t>(predicate);
	case PhysicalType::UINT64:
		return GetMatchFunction<NO_MATCH_SEL, uint8_t>(predicate);
	case PhysicalType::FLOAT:
		return GetMatchFunction<NO_MATCH_SEL, float>(predicate);
	case PhysicalType::DOUBLE:
		return GetMatchFunction<NO_MATCH_SEL, double>(predicate);
	case PhysicalType::INTERVAL:
		return GetMatchFunction<NO_MATCH_SEL, interval_t>(predicate);
	case PhysicalType::VARCHAR:
		return GetMatchFunction<NO_MATCH_SEL, string_t>(predicate);
	case PhysicalType::STRUCT:
		return GetStructMatchFunction<NO_MATCH_SEL>(type, predicate);
	case PhysicalType::LIST:
		return GetListMatchFunction<NO_MATCH_SEL>(predicate);
	default:
		throw InternalException("Unsupported PhysicalType for RowMatcher::GetMatchFunction: %s",
		                        EnumUtil::ToString(type.InternalType()));
	}
}

template <bool NO_MATCH_SEL, class T>
MatchFunction RowMatcher::GetMatchFunction(const ExpressionType predicate) {
	MatchFunction result;
	switch (predicate) {
	case ExpressionType::COMPARE_EQUAL:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, Equals>;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, NotEquals>;
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, DistinctFrom>;
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, NotDistinctFrom>;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, GreaterThan>;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, GreaterThanEquals>;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, LessThan>;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, LessThanEquals>;
		break;
	default:
		throw InternalException("Unsupported ExpressionType for RowMatcher::GetMatchFunction: %s",
		                        EnumUtil::ToString(predicate));
	}
	return result;
}

template <bool NO_MATCH_SEL>
MatchFunction RowMatcher::GetStructMatchFunction(const LogicalType &type, const ExpressionType predicate) {
	MatchFunction result;
	ExpressionType child_predicate = predicate;
	switch (predicate) {
	case ExpressionType::COMPARE_EQUAL:
		result.function = StructMatch<NO_MATCH_SEL, Equals, Equals>;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		result.function = StructMatch<NO_MATCH_SEL, NotEquals, NotEquals>;
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		result.function = StructMatch<NO_MATCH_SEL, DistinctFrom, NotEquals>;
		child_predicate = ExpressionType::COMPARE_NOTEQUAL;
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		result.function = StructMatch<NO_MATCH_SEL, NotDistinctFrom, Equals>;
		child_predicate = ExpressionType::COMPARE_EQUAL;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		result.function = StructMatch<NO_MATCH_SEL, GreaterThan, GreaterThan>;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		result.function = StructMatch<NO_MATCH_SEL, GreaterThanEquals, GreaterThanEquals>;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		result.function = StructMatch<NO_MATCH_SEL, LessThan, LessThan>;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		result.function = StructMatch<NO_MATCH_SEL, LessThanEquals, LessThanEquals>;
		break;
	default:
		throw InternalException("Unsupported ExpressionType for RowMatcher::GetStructMatchFunction: %s",
		                        EnumUtil::ToString(predicate));
	}

	result.child_functions.reserve(StructType::GetChildCount(type));
	for (const auto &child_type : StructType::GetChildTypes(type)) {
		result.child_functions.push_back(GetMatchFunction<NO_MATCH_SEL>(child_type.second, child_predicate));
	}
}

template <bool NO_MATCH_SEL>
MatchFunction RowMatcher::GetListMatchFunction(const ExpressionType predicate) {
	MatchFunction result;
	switch (predicate) {
	case ExpressionType::COMPARE_EQUAL:
		result.function = ListMatch<NO_MATCH_SEL, Equals>;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		result.function = ListMatch<NO_MATCH_SEL, NotEquals>;
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		result.function = ListMatch<NO_MATCH_SEL, DistinctFrom>;
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		result.function = ListMatch<NO_MATCH_SEL, NotDistinctFrom>;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		result.function = ListMatch<NO_MATCH_SEL, GreaterThan>;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		result.function = ListMatch<NO_MATCH_SEL, GreaterThanEquals>;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		result.function = ListMatch<NO_MATCH_SEL, LessThan>;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		result.function = ListMatch<NO_MATCH_SEL, LessThanEquals>;
		break;
	default:
		throw InternalException("Unsupported ExpressionType for RowMatcher::GetListMatchFunction: %s",
		                        EnumUtil::ToString(predicate));
	}
	return result;
}

} // namespace duckdb
