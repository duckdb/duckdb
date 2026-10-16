//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache/external_file_cache_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! Snapshot of the read statistics of the external file cache
struct ExternalFileCacheStatsInformation {
	//! Bytes requested by readers through the cache, clamped to the file size
	idx_t read_request_bytes;
	//! Requested bytes served from cached blocks
	idx_t cache_hit_request_bytes;
	//! Bytes read from the underlying file system, including alignment and prefetch beyond the request
	idx_t actual_io_bytes;
	//! Requested blocks served from the cache (memory, spill, or a concurrent reader's fetch)
	idx_t hit_count;
	//! Requested blocks fetched from the underlying file system
	idx_t miss_count;
	//! Evicted cache blocks that had to be fetched again
	idx_t eviction_refetch_count;
};

//! Read statistics of the external file cache, updated lock-free by readers
struct ExternalFileCacheStats {
	//! Bytes requested by readers through the cache, clamped to the file size
	atomic<idx_t> read_request_bytes {0};
	//! Requested bytes served from cached blocks
	atomic<idx_t> cache_hit_request_bytes {0};
	//! Bytes read from the underlying file system, including alignment and prefetch beyond the request
	atomic<idx_t> actual_io_bytes {0};
	//! Requested blocks served from the cache (memory, spill, or a concurrent reader's fetch)
	atomic<idx_t> hit_count {0};
	//! Requested blocks fetched from the underlying file system
	atomic<idx_t> miss_count {0};
	//! Evicted cache blocks that had to be fetched again
	atomic<idx_t> eviction_refetch_count {0};

	ExternalFileCacheStatsInformation GetSnapshot() const {
		return {
		    read_request_bytes.load(std::memory_order_relaxed), cache_hit_request_bytes.load(std::memory_order_relaxed),
		    actual_io_bytes.load(std::memory_order_relaxed),    hit_count.load(std::memory_order_relaxed),
		    miss_count.load(std::memory_order_relaxed),         eviction_refetch_count.load(std::memory_order_relaxed)};
	}
};

} // namespace duckdb
