//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/storage_block_prefetch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class StorageBlockPrefetch : uint8_t { REMOTE_ONLY = 0, NEVER = 1, ALWAYS_PREFETCH = 2, DEBUG_FORCE_ALWAYS = 3 };

} // namespace duckdb
