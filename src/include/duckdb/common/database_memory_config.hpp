//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/database_memory_config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Configuration shared by every database in a memory-management domain.
struct DatabaseMemoryConfig {
	DatabaseMemoryConfig() = default;
	DUCKDB_API DatabaseMemoryConfig(const DatabaseMemoryConfig &other);
	DUCKDB_API DatabaseMemoryConfig &operator=(const DatabaseMemoryConfig &other);

	//! The maximum memory used by the database system (in bytes). Default: 80% of available system memory.
	atomic<idx_t> maximum_memory {DConstants::INVALID_INDEX};
	//! Physical memory that the block allocator is allowed to use.
	atomic<idx_t> block_allocator_size {0};
	//! Record timestamps of buffer manager unpin() events. Usable by custom eviction policies.
	bool buffer_manager_track_eviction_timestamps = false;
	//! If bulk deallocation larger than this occurs, flush outstanding allocations.
	atomic<idx_t> allocator_bulk_deallocation_flush_threshold {536870912ULL};
};

} // namespace duckdb
