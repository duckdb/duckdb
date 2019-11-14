//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/checksum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Compute a checksum over a buffer of size size
uint64_t Checksum(uint8_t *buffer, size_t size);

} // namespace duckdb
