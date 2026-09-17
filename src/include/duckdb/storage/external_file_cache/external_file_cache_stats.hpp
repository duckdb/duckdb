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
	idx_t requested_bytes;
	//! Block reads served from the cache (memory or spill)
	idx_t hit_count;
	idx_t hit_bytes;
	//! Block reads that fetched from the underlying file system
	idx_t miss_count;
	idx_t miss_bytes;
	//! Subset of misses that re-fetched a previously loaded block that was evicted
	idx_t eviction_refetch_count;
};

//! Read statistics of the external file cache, updated lock-free by readers
struct ExternalFileCacheStats {
	//! Bytes requested by readers through the cache
	atomic<idx_t> requested_bytes {0};
	//! Block reads served from the cache (memory or spill)
	atomic<idx_t> hit_count {0};
	atomic<idx_t> hit_bytes {0};
	//! Block reads that fetched from the underlying file system
	atomic<idx_t> miss_count {0};
	atomic<idx_t> miss_bytes {0};
	//! Subset of misses that re-fetched a previously loaded block that was evicted
	atomic<idx_t> eviction_refetch_count {0};

	ExternalFileCacheStatsInformation GetSnapshot() const {
		return {requested_bytes, hit_count, hit_bytes, miss_count, miss_bytes, eviction_refetch_count};
	}
};

} // namespace duckdb
