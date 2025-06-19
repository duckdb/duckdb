//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/hash.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/datetime.hpp"

namespace duckdb {

struct string_t;
struct interval_t; // NOLINT

// efficient hash function that maximizes the avalanche effect and minimizes
// bias
// see: https://nullprogram.com/blog/2018/07/31/

inline hash_t MurmurHash64(uint64_t x) {
	x ^= x >> 32;
	x *= 0xd6e8feb86659fd93U;
	x ^= x >> 32;
	x *= 0xd6e8feb86659fd93U;
	x ^= x >> 32;
	return x;
}

inline hash_t MurmurHash32(uint32_t x) {
	return MurmurHash64(x);
}

template <class T>
hash_t Hash(T value) {
	return MurmurHash32(static_cast<uint32_t>(value));
}

//! Combine two hashes by XORing them
inline hash_t CombineHash(hash_t left, hash_t right) {
	return left ^ right;
}

template <>
DUCKDB_API hash_t Hash(uint64_t val);
template <>
DUCKDB_API hash_t Hash(int64_t val);
template <>
DUCKDB_API hash_t Hash(hugeint_t val);
template <>
DUCKDB_API hash_t Hash(uhugeint_t val);
template <>
DUCKDB_API hash_t Hash(float val);
template <>
DUCKDB_API hash_t Hash(double val);
template <>
DUCKDB_API hash_t Hash(const char *val);
template <>
DUCKDB_API hash_t Hash(char *val);
template <>
DUCKDB_API hash_t Hash(string_t val);
template <>
DUCKDB_API hash_t Hash(interval_t val);
template <>
DUCKDB_API hash_t Hash(dtime_tz_t val);
DUCKDB_API hash_t Hash(const char *val, size_t size);
DUCKDB_API hash_t Hash(uint8_t *val, size_t size);

} // namespace duckdb
