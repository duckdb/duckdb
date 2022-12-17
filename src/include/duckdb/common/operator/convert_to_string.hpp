//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/convert_to_string.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/type_util.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct ConvertToString {
	template <class SRC>
	DUCKDB_API static inline string Operation(SRC input) {
		throw InternalException("Unrecognized type for ConvertToString %s", GetTypeId<SRC>());
	}
};

template <>
string ConvertToString::Operation(bool input);
template <>
string ConvertToString::Operation(int8_t input);
template <>
string ConvertToString::Operation(int16_t input);
template <>
string ConvertToString::Operation(int32_t input);
template <>
string ConvertToString::Operation(int64_t input);
template <>
string ConvertToString::Operation(uint8_t input);
template <>
string ConvertToString::Operation(uint16_t input);
template <>
string ConvertToString::Operation(uint32_t input);
template <>
string ConvertToString::Operation(uint64_t input);
template <>
string ConvertToString::Operation(hugeint_t input);
template <>
string ConvertToString::Operation(float input);
template <>
string ConvertToString::Operation(double input);
template <>
string ConvertToString::Operation(interval_t input);
template <>
string ConvertToString::Operation(date_t input);
template <>
string ConvertToString::Operation(dtime_t input);
template <>
string ConvertToString::Operation(timestamp_t input);
template <>
string ConvertToString::Operation(string_t input);

} // namespace duckdb
