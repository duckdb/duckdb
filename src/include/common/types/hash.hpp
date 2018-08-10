//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/types/hash.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory.h>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

namespace duckdb {

// efficient hash function that maximizes the avalanche effect and minimizes
// bias
// see: https://nullprogram.com/blog/2018/07/31/
inline int32_t murmurhash32(uint32_t x) {
	x ^= x >> 16;
	x *= UINT32_C(0x85ebca6b);
	x ^= x >> 13;
	x *= UINT32_C(0xc2b2ae35);
	x ^= x >> 16;
	return *((int32_t *)&x);
}

// 64-bit hash function, XOR together two calls to 32-bit
inline int32_t murmurhash64(uint32_t *vals) {
	auto left = murmurhash32(vals[0]);
	auto right = murmurhash32(vals[1]);
	return left ^ right;
}

template <class T> int32_t Hash(T value) { return murmurhash32(value); }

template <> int32_t Hash(uint64_t val);
template <> int32_t Hash(int64_t val);
template <> int32_t Hash(double val);
template <> int32_t Hash(const char *val);
template <> int32_t Hash(char *val);

} // namespace duckdb
