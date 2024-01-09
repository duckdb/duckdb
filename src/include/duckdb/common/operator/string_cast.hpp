//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/string_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

//! StringCast
class Vector;

struct StringCast {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw NotImplementedException("Unimplemented type for string cast!");
	}
};

template <>
DUCKDB_API duckdb::string_t StringCast::Operation(bool input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(int8_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(int16_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(int32_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(int64_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(uint8_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(uint16_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(uint32_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(uint64_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(hugeint_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(float input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(double input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(interval_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(duckdb::string_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(date_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(dtime_t input, Vector &result);
template <>
DUCKDB_API duckdb::string_t StringCast::Operation(timestamp_t input, Vector &result);

//! Temporary casting for Time Zone types. TODO: turn casting into functions.
struct StringCastTZ {
	template <typename SRC>
	static inline string_t Operation(SRC input, Vector &vector) {
		return StringCast::Operation(input, vector);
	}
};

template <>
duckdb::string_t StringCastTZ::Operation(date_t input, Vector &result);
template <>
duckdb::string_t StringCastTZ::Operation(dtime_tz_t input, Vector &result);
template <>
duckdb::string_t StringCastTZ::Operation(timestamp_t input, Vector &result);

} // namespace duckdb
