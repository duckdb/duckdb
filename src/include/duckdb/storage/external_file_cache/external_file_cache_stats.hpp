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
	//! Bytes requested by readers through the cache
	idx_t read_request_bytes;
	//! Requested bytes served from cached blocks
	idx_t cache_hit_request_bytes;
	//! Bytes read from the underlying file system due to cache misses
	idx_t actual_io_bytes;
	//! Block reads served from the cache (memory or spill)
	idx_t hit_count;
	//! Block reads that fetched from the underlying file system
	idx_t miss_count;
	//! Subset of misses that re-fetched a previously loaded block that was evicted
	idx_t eviction_refetch_count;
};

//! Read statistics of the external file cache, updated lock-free by readers
struct ExternalFileCacheStats {
	//! Bytes requested by readers through the cache
	atomic<idx_t> read_request_bytes {0};
	//! Requested bytes served from cached blocks
	atomic<idx_t> cache_hit_request_bytes {0};
	//! Bytes read from the underlying file system due to cache misses
	atomic<idx_t> actual_io_bytes {0};
	//! Block reads served from the cache (memory or spill)
	atomic<idx_t> hit_count {0};
	//! Block reads that fetched from the underlying file system
	atomic<idx_t> miss_count {0};
	//! Subset of misses that re-fetched a previously loaded block that was evicted
	atomic<idx_t> eviction_refetch_count {0};

	ExternalFileCacheStatsInformation GetSnapshot() const {
		return {
		    read_request_bytes.load(std::memory_order_relaxed), cache_hit_request_bytes.load(std::memory_order_relaxed),
		    actual_io_bytes.load(std::memory_order_relaxed),    hit_count.load(std::memory_order_relaxed),
		    miss_count.load(std::memory_order_relaxed),         eviction_refetch_count.load(std::memory_order_relaxed)};
	}
};

} // namespace duckdb
