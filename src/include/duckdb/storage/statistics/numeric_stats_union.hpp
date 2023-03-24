//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/numeric_stats_union.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

struct NumericValueUnion {
	union Val {
		bool boolean;
		int8_t tinyint;
		int16_t smallint;
		int32_t integer;
		int64_t bigint;
		uint8_t utinyint;
		uint16_t usmallint;
		uint32_t uinteger;
		uint64_t ubigint;
		hugeint_t hugeint;
		float float_;
		double double_;
	} value_;

	template <class T>
	T &GetReferenceUnsafe();
};

template <>
DUCKDB_API bool &NumericValueUnion::GetReferenceUnsafe();
template <>
DUCKDB_API int8_t &NumericValueUnion::GetReferenceUnsafe();
template <>
DUCKDB_API int16_t &NumericValueUnion::GetReferenceUnsafe();
template <>
DUCKDB_API int32_t &NumericValueUnion::GetReferenceUnsafe();
template <>
DUCKDB_API int64_t &NumericValueUnion::GetReferenceUnsafe();
template <>
DUCKDB_API hugeint_t &NumericValueUnion::GetReferenceUnsafe();
template <>
DUCKDB_API uint8_t &NumericValueUnion::GetReferenceUnsafe();
template <>
DUCKDB_API uint16_t &NumericValueUnion::GetReferenceUnsafe();
template <>
DUCKDB_API uint32_t &NumericValueUnion::GetReferenceUnsafe();
template <>
DUCKDB_API uint64_t &NumericValueUnion::GetReferenceUnsafe();
template <>
DUCKDB_API float &NumericValueUnion::GetReferenceUnsafe();
template <>
DUCKDB_API double &NumericValueUnion::GetReferenceUnsafe();

} // namespace duckdb
