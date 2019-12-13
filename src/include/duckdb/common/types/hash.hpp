//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/hash.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

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

inline uint64_t murmurhash64(uint64_t x) {
	x ^= x >> 30;
	x *= UINT64_C(0xbf58476d1ce4e5b9);
	x ^= x >> 27;
	x *= UINT64_C(0x94d049bb133111eb);
	x ^= x >> 31;
	return x;
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
template <> uint64_t Hash(float val);
template <> uint64_t Hash(double val);
template <> uint64_t Hash(const char *val);
template <> uint64_t Hash(char *val);
uint64_t Hash(const char *val, size_t size);
uint64_t Hash(char *val, size_t size);
uint64_t Hash(uint8_t *val, size_t size);

} // namespace duckdb
