//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi/cast/utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Unsafe Fetch (for internal use only)
//===--------------------------------------------------------------------===//
template <class T>
T UnsafeFetchFromPtr(void *pointer) {
	return *((T *)pointer);
}

template <class T>
void *UnsafeFetchPtr(duckdb_result *result, idx_t col, idx_t row) {
	D_ASSERT(row < result->__deprecated_row_count);
	return (void *)&(((T *)result->__deprecated_columns[col].__deprecated_data)[row]);
}

template <class T>
T UnsafeFetch(duckdb_result *result, idx_t col, idx_t row) {
	return UnsafeFetchFromPtr<T>(UnsafeFetchPtr<T>(result, col, row));
}

//===--------------------------------------------------------------------===//
// Fetch Default Value
//===--------------------------------------------------------------------===//
struct FetchDefaultValue {
	template <class T>
	static T Operation() {
		return 0;
	}
};

template <>
duckdb_decimal FetchDefaultValue::Operation();
template <>
date_t FetchDefaultValue::Operation();
template <>
dtime_t FetchDefaultValue::Operation();
template <>
timestamp_t FetchDefaultValue::Operation();
template <>
interval_t FetchDefaultValue::Operation();
template <>
char *FetchDefaultValue::Operation();
template <>
duckdb_string FetchDefaultValue::Operation();
template <>
duckdb_blob FetchDefaultValue::Operation();

//===--------------------------------------------------------------------===//
// String Casts
//===--------------------------------------------------------------------===//
template <class OP>
struct FromCStringCastWrapper {
	template <class SOURCE_TYPE, class RESULT_TYPE>
	static bool Operation(SOURCE_TYPE input_str, RESULT_TYPE &result) {
		string_t input(input_str);
		return OP::template Operation<string_t, RESULT_TYPE>(input, result);
	}
};

template <class OP>
struct ToCStringCastWrapper {
	template <class SOURCE_TYPE, class RESULT_TYPE>
	static bool Operation(SOURCE_TYPE input, RESULT_TYPE &result) {
		Vector result_vector(LogicalType::VARCHAR, nullptr);
		auto result_string = OP::template Operation<SOURCE_TYPE>(input, result_vector);
		auto result_size = result_string.GetSize();
		auto result_data = result_string.GetData();

		char *allocated_data = char_ptr_cast(duckdb_malloc(result_size + 1));
		memcpy(allocated_data, result_data, result_size);
		allocated_data[result_size] = '\0';
		result.data = allocated_data;
		result.size = result_size;
		return true;
	}
};

//===--------------------------------------------------------------------===//
// Blob Casts
//===--------------------------------------------------------------------===//
struct FromCBlobCastWrapper {
	template <class SOURCE_TYPE, class RESULT_TYPE>
	static bool Operation(SOURCE_TYPE input_str, RESULT_TYPE &result) {
		return false;
	}
};

template <>
bool FromCBlobCastWrapper::Operation(duckdb_blob input, duckdb_string &result);

template <class SOURCE_TYPE, class RESULT_TYPE, class OP>
RESULT_TYPE TryCastCInternal(duckdb_result *result, idx_t col, idx_t row) {
	RESULT_TYPE result_value;
	try {
		if (!OP::template Operation<SOURCE_TYPE, RESULT_TYPE>(UnsafeFetch<SOURCE_TYPE>(result, col, row),
		                                                      result_value)) {
			return FetchDefaultValue::Operation<RESULT_TYPE>();
		}
	} catch (...) {
		return FetchDefaultValue::Operation<RESULT_TYPE>();
	}
	return result_value;
}

} // namespace duckdb

bool CanFetchValue(duckdb_result *result, idx_t col, idx_t row);
bool CanUseDeprecatedFetch(duckdb_result *result, idx_t col, idx_t row);
