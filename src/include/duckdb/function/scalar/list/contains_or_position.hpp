#pragma once
#include "duckdb/core_functions/create_sort_key.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

template <class T, bool RETURN_POSITION>
idx_t ListSearchSimpleOp(Vector &list_vec, Vector &source_vec, Vector &target_vec, Vector &result, idx_t count) {

	UnifiedVectorFormat source_format;
	const auto source_count = ListVector::GetListSize(list_vec);
	source_vec.ToUnifiedFormat(source_count, source_format);
	const auto source_data = UnifiedVectorFormat::GetData<T>(source_format);

	using RETURN_TYPE = typename std::conditional<RETURN_POSITION, int32_t, int8_t>::type;

	idx_t total_matches = 0;

	BinaryExecutor::ExecuteWithNulls<list_entry_t, T, RETURN_TYPE>(
	    list_vec, target_vec, result, count,
	    [&](const list_entry_t &list, const T &target, ValidityMask &validity, idx_t out_idx) -> RETURN_TYPE {
		    if (list.length == 0) {
			    if (RETURN_POSITION) {
				    validity.SetInvalid(out_idx);
			    }
			    return 0;
		    }

		    for (auto i = list.offset; i < list.offset + list.length; i++) {
			    const auto entry_idx = source_format.sel->get_index(i);
			    if (source_format.validity.RowIsValid(entry_idx) &&
			        Equals::Operation<T>(source_data[entry_idx], target)) {
				    total_matches++;
				    return RETURN_POSITION ? UnsafeNumericCast<RETURN_TYPE>(1 + i - list.offset) : 1;
			    }
		    }

		    if (RETURN_POSITION) {
			    validity.SetInvalid(out_idx);
		    }
		    return 0;
	    });

	return total_matches;
}

template <bool RETURN_POSITION>
idx_t ListSearchNestedOp(Vector &list_vec, Vector &source_vec, Vector &target_vec, Vector &result_vec,
                         const idx_t target_count) {
	// Set up sort keys for nested types.
	auto source_count = ListVector::GetListSize(list_vec);
	Vector source_sort_key_vec(LogicalType::BLOB, source_count);
	Vector target_sort_key_vec(LogicalType::BLOB, target_count);

	const OrderModifiers order_modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
	CreateSortKeyHelpers::CreateSortKeyWithValidity(source_vec, source_sort_key_vec, order_modifiers, source_count);
	CreateSortKeyHelpers::CreateSortKeyWithValidity(target_vec, target_sort_key_vec, order_modifiers, target_count);

	return ListSearchSimpleOp<string_t, RETURN_POSITION>(list_vec, source_sort_key_vec, target_sort_key_vec, result_vec,
	                                                     target_count);
}

//! "Search" each list in the list vector for the corresponding value in the target vector, returning either
//! true/false or the position of the value in the list. The result vector is populated with the result of the search.
//! usually the "source" vector is the list child vector, but it is passed separately to enable searching nested
//! children, for example when searching the keys of a MAP vectors.
template <bool RETURN_POSITION>
idx_t ListSearchOp(Vector &list_v, Vector &source_v, Vector &target_v, Vector &result_v, idx_t target_count) {
	const auto type = target_v.GetType().InternalType();
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return ListSearchSimpleOp<int8_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::INT16:
		return ListSearchSimpleOp<int16_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::INT32:
		return ListSearchSimpleOp<int32_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::INT64:
		return ListSearchSimpleOp<int64_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::INT128:
		return ListSearchSimpleOp<hugeint_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::UINT8:
		return ListSearchSimpleOp<uint8_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::UINT16:
		return ListSearchSimpleOp<uint16_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::UINT32:
		return ListSearchSimpleOp<uint32_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::UINT64:
		return ListSearchSimpleOp<uint64_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::UINT128:
		return ListSearchSimpleOp<uhugeint_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::FLOAT:
		return ListSearchSimpleOp<float, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::DOUBLE:
		return ListSearchSimpleOp<double, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::VARCHAR:
		return ListSearchSimpleOp<string_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::INTERVAL:
		return ListSearchSimpleOp<interval_t, RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
	case PhysicalType::ARRAY:
		return ListSearchNestedOp<RETURN_POSITION>(list_v, source_v, target_v, result_v, target_count);
	default:
		throw NotImplementedException("This function has not been implemented for logical type %s",
		                              TypeIdToString(type));
	}
}

} // namespace duckdb
