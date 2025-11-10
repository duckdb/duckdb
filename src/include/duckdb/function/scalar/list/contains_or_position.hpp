#pragma once
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

template <class T, class RETURN_TYPE, bool FIND_NULLS>
idx_t ListSearchSimpleOp(Vector &input_list, Vector &list_child, Vector &target, Vector &result, const idx_t count) {
	// If the return type is not a bool, return the position
	const auto return_pos = std::is_same<RETURN_TYPE, int32_t>::value;

	const auto input_count = ListVector::GetListSize(input_list);

	UnifiedVectorFormat list_format;
	input_list.ToUnifiedFormat(count, list_format);
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_format);

	UnifiedVectorFormat child_format;
	list_child.ToUnifiedFormat(input_count, child_format);
	const auto child_data = UnifiedVectorFormat::GetData<T>(child_format);

	UnifiedVectorFormat target_format;
	target.ToUnifiedFormat(count, target_format);
	const auto target_data = UnifiedVectorFormat::GetData<T>(target_format);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<RETURN_TYPE>(result);
	auto &result_validity = FlatVector::Validity(result);

	idx_t total_matches = 0;

	for (idx_t row_idx = 0; row_idx < count; ++row_idx) {
		const auto list_entry_idx = list_format.sel->get_index(row_idx);

		// The entire list is NULL, the result is also NULL.
		if (!list_format.validity.RowIsValid(list_entry_idx)) {
			result_validity.SetInvalid(row_idx);
			continue;
		}

		const auto target_entry_idx = target_format.sel->get_index(row_idx);
		const bool target_valid = target_format.validity.RowIsValid(target_entry_idx);

		// We are finished, if we are not looking for NULL, and the target is NULL.
		const auto finished = !FIND_NULLS && !target_valid;
		// We did not find the target (finished, or list is empty).
		if (finished || list_entries[list_entry_idx].length == 0) {
			if (finished || return_pos) {
				// Return NULL as the position.
				result_validity.SetInvalid(row_idx);
			} else {
				// Set 'contains' to false.
				result_data[row_idx] = false;
			}
			continue;
		}

		const auto entry_length = list_entries[list_entry_idx].length;
		const auto entry_offset = list_entries[list_entry_idx].offset;

		bool found = false;

		for (auto list_idx = entry_offset; list_idx < entry_length + entry_offset && !found; list_idx++) {
			const auto child_entry_idx = child_format.sel->get_index(list_idx);
			const bool child_valid = child_format.validity.RowIsValid(child_entry_idx);

			if ((FIND_NULLS && !child_valid && !target_valid) ||
			    (child_valid && target_valid &&
			     Equals::Operation<T>(child_data[child_entry_idx], target_data[target_entry_idx]))) {
				found = true;
				total_matches++;
				if (return_pos) {
					result_data[row_idx] = UnsafeNumericCast<int32_t>(1 + list_idx - entry_offset);
				} else {
					result_data[row_idx] = true;
				}
			}
		}

		if (!found) {
			if (return_pos) {
				result_validity.SetInvalid(row_idx);
			} else {
				result_data[row_idx] = false;
			}
		}
	}

	return total_matches;
}

template <class RETURN_TYPE, bool FIND_NULLS>
idx_t ListSearchNestedOp(Vector &list_vec, Vector &source_vec, Vector &target_vec, Vector &result_vec,
                         const idx_t target_count) {
	// Set up sort keys for nested types.
	auto source_count = ListVector::GetListSize(list_vec);
	Vector source_sort_key_vec(LogicalType::BLOB, source_count);
	Vector target_sort_key_vec(LogicalType::BLOB, target_count);

	const OrderModifiers order_modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
	CreateSortKeyHelpers::CreateSortKeyWithValidity(source_vec, source_sort_key_vec, order_modifiers, source_count);
	CreateSortKeyHelpers::CreateSortKeyWithValidity(target_vec, target_sort_key_vec, order_modifiers, target_count);

	return ListSearchSimpleOp<string_t, RETURN_TYPE, FIND_NULLS>(list_vec, source_sort_key_vec, target_sort_key_vec,
	                                                             result_vec, target_count);
}

//! "Search" each list in the list vector for the corresponding value in the target vector, returning either
//! true/false or the position of the value in the list. The result vector is populated with the result of the search.
//! usually the "source" vector is the list child vector, but it is passed separately to enable searching nested
//! children, for example when searching the keys of a MAP vectors.
template <class RETURN_TYPE, bool FIND_NULLS = false>
idx_t ListSearchOp(Vector &list_v, Vector &source_v, Vector &target_v, Vector &result_v, idx_t target_count) {
	const auto type = target_v.GetType().InternalType();
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return ListSearchSimpleOp<int8_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::INT16:
		return ListSearchSimpleOp<int16_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::INT32:
		return ListSearchSimpleOp<int32_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::INT64:
		return ListSearchSimpleOp<int64_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::INT128:
		return ListSearchSimpleOp<hugeint_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v,
		                                                              target_count);
	case PhysicalType::UINT8:
		return ListSearchSimpleOp<uint8_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::UINT16:
		return ListSearchSimpleOp<uint16_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v,
		                                                             target_count);
	case PhysicalType::UINT32:
		return ListSearchSimpleOp<uint32_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v,
		                                                             target_count);
	case PhysicalType::UINT64:
		return ListSearchSimpleOp<uint64_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v,
		                                                             target_count);
	case PhysicalType::UINT128:
		return ListSearchSimpleOp<uhugeint_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v,
		                                                               target_count);
	case PhysicalType::FLOAT:
		return ListSearchSimpleOp<float, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::DOUBLE:
		return ListSearchSimpleOp<double, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::VARCHAR:
		return ListSearchSimpleOp<string_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v,
		                                                             target_count);
	case PhysicalType::INTERVAL:
		return ListSearchSimpleOp<interval_t, RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v,
		                                                               target_count);
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
	case PhysicalType::ARRAY:
		return ListSearchNestedOp<RETURN_TYPE, FIND_NULLS>(list_v, source_v, target_v, result_v, target_count);
	default:
		throw NotImplementedException("This function has not been implemented for logical type %s",
		                              TypeIdToString(type));
	}
}

} // namespace duckdb
