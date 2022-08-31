//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi/capi_cast_from_decimal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/capi/cast/generic.hpp"

namespace duckdb {

template <class RESULT_TYPE>
static bool CastDecimalCInternal(duckdb_result *source, RESULT_TYPE &result, idx_t col, idx_t row) {
	auto result_data = (duckdb::DuckDBResultData *)source->internal_data;
	auto &query_result = result_data->result;
	auto &source_type = query_result->types[col];
	auto width = duckdb::DecimalType::GetWidth(source_type);
	auto scale = duckdb::DecimalType::GetScale(source_type);
	void *source_address = UnsafeFetchPtr<hugeint_t>(source, col, row);
	switch (source_type.InternalType()) {
	case duckdb::PhysicalType::INT16:
		return duckdb::TryCastFromDecimal::Operation<int16_t, RESULT_TYPE>(UnsafeFetchFromPtr<int16_t>(source_address),
		                                                                   result, nullptr, width, scale);
	case duckdb::PhysicalType::INT32:
		return duckdb::TryCastFromDecimal::Operation<int32_t, RESULT_TYPE>(UnsafeFetchFromPtr<int32_t>(source_address),
		                                                                   result, nullptr, width, scale);
	case duckdb::PhysicalType::INT64:
		return duckdb::TryCastFromDecimal::Operation<int64_t, RESULT_TYPE>(UnsafeFetchFromPtr<int64_t>(source_address),
		                                                                   result, nullptr, width, scale);
	case duckdb::PhysicalType::INT128:
		return duckdb::TryCastFromDecimal::Operation<hugeint_t, RESULT_TYPE>(
		    UnsafeFetchFromPtr<hugeint_t>(source_address), result, nullptr, width, scale);
	default:
		throw duckdb::InternalException("Unimplemented internal type for decimal");
	}
}

template <>
bool CastDecimalCInternal(duckdb_result *source, char *&result, idx_t col, idx_t row) {
	auto result_data = (duckdb::DuckDBResultData *)source->internal_data;
	auto &query_result = result_data->result;
	auto &source_type = query_result->types[col];
	auto width = duckdb::DecimalType::GetWidth(source_type);
	auto scale = duckdb::DecimalType::GetScale(source_type);
	duckdb::Vector result_vec(duckdb::LogicalType::VARCHAR, false, false);
	duckdb::string_t result_string;
	switch (source_type.InternalType()) {
	case duckdb::PhysicalType::INT16:
		result_string = duckdb::StringCastFromDecimal::Operation<int16_t>(UnsafeFetch<int16_t>(source, col, row), width,
		                                                                  scale, result_vec);
		break;
	case duckdb::PhysicalType::INT32:
		result_string = duckdb::StringCastFromDecimal::Operation<int32_t>(UnsafeFetch<int32_t>(source, col, row), width,
		                                                                  scale, result_vec);
		break;
	case duckdb::PhysicalType::INT64:
		result_string = duckdb::StringCastFromDecimal::Operation<int64_t>(UnsafeFetch<int64_t>(source, col, row), width,
		                                                                  scale, result_vec);
		break;
	case duckdb::PhysicalType::INT128:
		result_string = duckdb::StringCastFromDecimal::Operation<hugeint_t>(UnsafeFetch<hugeint_t>(source, col, row),
		                                                                    width, scale, result_vec);
		break;
	default:
		throw duckdb::InternalException("Unimplemented internal type for decimal");
	}
	result = (char *)duckdb_malloc(sizeof(char) * (result_string.GetSize() + 1));
	memcpy(result, result_string.GetDataUnsafe(), result_string.GetSize());
	result[result_string.GetSize()] = '\0';
	return true;
}

template <>
bool CastDecimalCInternal(duckdb_result *source, duckdb_decimal &result, idx_t col, idx_t row) {
	auto result_data = (duckdb::DuckDBResultData *)source->internal_data;
	result_data->result->types[col].GetDecimalProperties(result.width, result.scale);

	auto internal_value = TryCastCInternal<hugeint_t, hugeint_t, duckdb::TryCast>(source, col, row);
	result.value.lower = internal_value.lower;
	result.value.upper = internal_value.upper;
	return true;
}

template <class RESULT_TYPE>
RESULT_TYPE TryCastDecimalCInternal(duckdb_result *source, idx_t col, idx_t row) {
	RESULT_TYPE result_value;
	try {
		if (!CastDecimalCInternal<RESULT_TYPE>(source, result_value, col, row)) {
			return FetchDefaultValue::Operation<RESULT_TYPE>();
		}
	} catch (...) {
		return FetchDefaultValue::Operation<RESULT_TYPE>();
	}
	return result_value;
}

} // namespace duckdb
