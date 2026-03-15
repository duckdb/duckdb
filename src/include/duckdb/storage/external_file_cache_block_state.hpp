//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache_block_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

enum class CacheBlockState : uint8_t {
	// initialize state, no data, no one fetching
	EMPTY,
	// a thread is actively performing I/O
	LOADING,
	// data available in block_handle (may be evicted by buffer manager)
	LOADED,
	// I/O failed, error_message contains the reason
	ERROR
};

} // namespace duckdb
