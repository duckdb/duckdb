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
		uhugeint_t uhugeint;
		float float_;   // NOLINT
		double double_; // NOLINT
	} value_;           // NOLINT

	template <class T>
	T &GetReferenceUnsafe();
};

template <>
DUCKDB_API inline bool &NumericValueUnion::GetReferenceUnsafe() {
	return value_.boolean;
}

template <>
DUCKDB_API inline int8_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.tinyint;
}

template <>
DUCKDB_API inline int16_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.smallint;
}

template <>
DUCKDB_API inline int32_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.integer;
}

template <>
DUCKDB_API inline int64_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.bigint;
}

template <>
DUCKDB_API inline hugeint_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.hugeint;
}

template <>
DUCKDB_API inline uhugeint_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.uhugeint;
}

template <>
DUCKDB_API inline uint8_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.utinyint;
}

template <>
DUCKDB_API inline uint16_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.usmallint;
}

template <>
DUCKDB_API inline uint32_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.uinteger;
}

template <>
DUCKDB_API inline uint64_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.ubigint;
}

template <>
DUCKDB_API inline float &NumericValueUnion::GetReferenceUnsafe() {
	return value_.float_;
}

template <>
DUCKDB_API inline double &NumericValueUnion::GetReferenceUnsafe() {
	return value_.double_;
}

} // namespace duckdb
