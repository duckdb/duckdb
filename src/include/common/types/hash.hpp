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

//! Hashes a bool to a 32-bit integer hash
int32_t Hash(bool integer);
//! Hashes an int8_t to a 32-bit integer hash
int32_t Hash(int8_t integer);
//! Hashes an int16_t to a 32-bit integer hash
int32_t Hash(int16_t integer);
//! Hashes an int32_t to a 32-bit integer hash
int32_t Hash(int32_t integer);
//! Hashes an int64_t to a 32-bit integer hash
int32_t Hash(int64_t integer);
//! Hashes an uint64_t to a 32-bit integer hash
int32_t Hash(uint64_t integer);
//! Hashes an double to a 32-bit integer hash
int32_t Hash(double integer);
//! Hashes a character string to a 32-bit integer hash
int32_t Hash(const char *str);

} // namespace duckdb
