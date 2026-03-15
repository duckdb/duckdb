//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache_block.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/storage/external_file_cache_block_state.hpp"

#include <condition_variable>

namespace duckdb {

// Forward declaration.
class BlockHandle;

struct CacheBlock {
	mutable annotated_mutex mtx;
	mutable std::condition_variable cv DUCKDB_GUARDED_BY(mtx);
	CacheBlockState state DUCKDB_GUARDED_BY(mtx) = CacheBlockState::EMPTY;
	shared_ptr<BlockHandle> block_handle DUCKDB_GUARDED_BY(mtx);
};

} // namespace duckdb
