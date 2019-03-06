//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types/hash.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

#include <memory.h>

namespace duckdb {

// efficient hash function that maximizes the avalanche effect and minimizes
// bias
// see: https://nullprogram.com/blog/2018/07/31/
inline uint64_t murmurhash32(uint32_t x) {
	x ^= x >> 16;
	x *= UINT32_C(0x85ebca6b);
	x ^= x >> 13;
	x *= UINT32_C(0xc2b2ae35);
	x ^= x >> 16;
	return (uint64_t)x;
}

// 64-bit hash function, XOR together two calls to 32-bit
inline uint64_t murmurhash64(uint32_t *vals) {
	auto left = murmurhash32(vals[0]);
	auto right = murmurhash32(vals[1]);
	return left ^ right;
}

template <class T> uint64_t Hash(T value) {
	return murmurhash32(value);
}

//! Combine two hashes by XORing them
inline uint64_t CombineHash(uint64_t left, uint64_t right) {
	return left ^ right;
}

template <> uint64_t Hash(uint64_t val);
template <> uint64_t Hash(int64_t val);
template <> uint64_t Hash(double val);
template <> uint64_t Hash(const char *val);
template <> uint64_t Hash(char *val);

} // namespace duckdb
