//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi/capi_cast_from_decimal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/capi/cast/utils.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

//! DECIMAL -> ?
//! The logical type a C API fetch of RESULT_TYPE produces, used to convert wide decimals through the Value API
template <class RESULT_TYPE>
LogicalType CResultLogicalType() {
	return LogicalType(LogicalTypeId::INVALID);
}
template <>
inline LogicalType CResultLogicalType<bool>() {
	return LogicalType::BOOLEAN;
}
template <>
inline LogicalType CResultLogicalType<int8_t>() {
	return LogicalType::TINYINT;
}
template <>
inline LogicalType CResultLogicalType<int16_t>() {
	return LogicalType::SMALLINT;
}
template <>
inline LogicalType CResultLogicalType<int32_t>() {
	return LogicalType::INTEGER;
}
template <>
inline LogicalType CResultLogicalType<int64_t>() {
	return LogicalType::BIGINT;
}
template <>
inline LogicalType CResultLogicalType<uint8_t>() {
	return LogicalType::UTINYINT;
}
template <>
inline LogicalType CResultLogicalType<uint16_t>() {
	return LogicalType::USMALLINT;
}
template <>
inline LogicalType CResultLogicalType<uint32_t>() {
	return LogicalType::UINTEGER;
}
template <>
inline LogicalType CResultLogicalType<uint64_t>() {
	return LogicalType::UBIGINT;
}
template <>
inline LogicalType CResultLogicalType<hugeint_t>() {
	return LogicalType::HUGEINT;
}
template <>
inline LogicalType CResultLogicalType<uhugeint_t>() {
	return LogicalType::UHUGEINT;
}
template <>
inline LogicalType CResultLogicalType<float>() {
	return LogicalType::FLOAT;
}
template <>
inline LogicalType CResultLogicalType<double>() {
	return LogicalType::DOUBLE;
}

template <class RESULT_TYPE>
bool CastDecimalCInternal(duckdb_result *source, RESULT_TYPE &result, idx_t col, idx_t row) {
	auto result_data = (duckdb::DuckDBResultData *)source->internal_data;
	auto &query_result = result_data->result;
	auto &source_type = query_result->GetTypes()[col];
	auto width = duckdb::DecimalType::GetWidth(source_type);
	auto scale = duckdb::DecimalType::GetScale(source_type);
	auto source_value = UnsafeFetch<hugeint_t>(source, col, row);
	CastParameters parameters;
	switch (source_type.InternalType()) {
	case duckdb::PhysicalType::INT16:
		return duckdb::TryCastFromDecimal::Operation<int16_t, RESULT_TYPE>(static_cast<int16_t>(source_value), result,
		                                                                   parameters, width, scale);
	case duckdb::PhysicalType::INT32:
		return duckdb::TryCastFromDecimal::Operation<int32_t, RESULT_TYPE>(static_cast<int32_t>(source_value), result,
		                                                                   parameters, width, scale);
	case duckdb::PhysicalType::INT64:
		return duckdb::TryCastFromDecimal::Operation<int64_t, RESULT_TYPE>(static_cast<int64_t>(source_value), result,
		                                                                   parameters, width, scale);
	case duckdb::PhysicalType::INT128:
		return duckdb::TryCastFromDecimal::Operation<hugeint_t, RESULT_TYPE>(source_value, result, parameters, width,
		                                                                     scale);
	case duckdb::PhysicalType::VARCHAR: {
		// wide decimal: the materialized duckdb_decimal only holds values that fit in 128 bits, so convert the
		// original value instead
		auto value = query_result->Collection().GetValue(col, row);
		if (value.IsNull()) {
			return false;
		}
		auto converted = value.DefaultTryCastAs(CResultLogicalType<RESULT_TYPE>());
		if (!converted) {
			return false;
		}
		result = converted->template GetValue<RESULT_TYPE>();
		return true;
	}
	default:
		throw duckdb::InternalException("Unimplemented internal type for decimal");
	}
}

//! DECIMAL -> VARCHAR
template <>
bool CastDecimalCInternal(duckdb_result *source, duckdb_string &result, idx_t col, idx_t row);

//! DECIMAL -> DECIMAL (internal fetch)
template <>
bool CastDecimalCInternal(duckdb_result *source, duckdb_decimal &result, idx_t col, idx_t row);

//! DECIMAL -> ...
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
