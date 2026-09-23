//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache/external_file_cache_block.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/storage/external_file_cache/external_file_cache_block_state.hpp"

#include <condition_variable>

namespace duckdb {

// Forward declaration.
class BlockHandle;

//! A cached byte range of a file. The blocks of a file never overlap and are sized by the reads that created them,
//! so the cache holds the requested bytes rather than aligned blocks around them.
struct CacheBlock {
	CacheBlock(idx_t location_p, idx_t size_p) : location(location_p), size(size_p) {
	}

	//! File offset of the first byte of the block
	const idx_t location;
	//! Number of file bytes the block covers
	const idx_t size;

	mutable annotated_mutex mtx;
	mutable std::condition_variable cv DUCKDB_GUARDED_BY(mtx);
	CacheBlockState state DUCKDB_GUARDED_BY(mtx) = CacheBlockState::EMPTY;
	shared_ptr<BlockHandle> block_handle DUCKDB_GUARDED_BY(mtx);
	//! Number of valid bytes that were read into this block
	idx_t nr_bytes DUCKDB_GUARDED_BY(mtx) = 0;
#ifdef DEBUG
	//! Checksum over the buffer contents, used for verifying data was not modified after caching
	hash_t checksum DUCKDB_GUARDED_BY(mtx) = 0;
#endif
};

} // namespace duckdb
