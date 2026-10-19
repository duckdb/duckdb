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
	idx_t block_hit_count;
	//! Subset of block hits that waited for a concurrent reader's fetch
	idx_t block_concurrent_hit_count;
	//! Requested blocks fetched from the underlying file system
	idx_t block_miss_count;
	//! Evicted cache blocks that had to be fetched again
	idx_t eviction_refetch_count;
};

//! Read statistics of the external file cache, cumulative since database start and updated lock-free by readers
struct ExternalFileCacheStats {
	//! Bytes requested by readers through the cache, clamped to the file size
	atomic<idx_t> read_request_bytes {0};
	//! Requested bytes served from cached blocks
	atomic<idx_t> cache_hit_request_bytes {0};
	//! Bytes read from the underlying file system, including alignment and prefetch beyond the request
	atomic<idx_t> actual_io_bytes {0};
	//! Requested blocks served from the cache (memory, spill, or a concurrent reader's fetch)
	atomic<idx_t> block_hit_count {0};
	//! Subset of block hits that waited for a concurrent reader's fetch
	atomic<idx_t> block_concurrent_hit_count {0};
	//! Requested blocks fetched from the underlying file system
	atomic<idx_t> block_miss_count {0};
	//! Evicted cache blocks that had to be fetched again
	atomic<idx_t> eviction_refetch_count {0};

	ExternalFileCacheStatsInformation GetSnapshot() const {
		ExternalFileCacheStatsInformation result;
		result.read_request_bytes = read_request_bytes.load(std::memory_order_relaxed);
		result.cache_hit_request_bytes = cache_hit_request_bytes.load(std::memory_order_relaxed);
		result.actual_io_bytes = actual_io_bytes.load(std::memory_order_relaxed);
		result.block_hit_count = block_hit_count.load(std::memory_order_relaxed);
		result.block_concurrent_hit_count = block_concurrent_hit_count.load(std::memory_order_relaxed);
		result.block_miss_count = block_miss_count.load(std::memory_order_relaxed);
		result.eviction_refetch_count = eviction_refetch_count.load(std::memory_order_relaxed);
		return result;
	}
};

} // namespace duckdb
