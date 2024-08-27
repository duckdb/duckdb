//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/destroy_buffer_upon.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DestroyBufferUpon : uint8_t {
	BLOCK = 0,    //! Destroy the data buffer upon destroying the associated BlockHandle
	EVICTION = 1, //! Destroy the data buffer upon eviction to storage
	UNPIN = 2     //! Destroy the data buffer upon unpin
};

} // namespace duckdb
