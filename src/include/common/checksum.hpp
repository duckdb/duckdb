//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/checksum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

//! Compute a checksum over a buffer of size size
uint64_t Checksum(char *buffer, size_t size);

}
