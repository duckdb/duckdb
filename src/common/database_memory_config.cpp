#include "duckdb/common/database_memory_config.hpp"

namespace duckdb {

DatabaseMemoryConfig::DatabaseMemoryConfig(const DatabaseMemoryConfig &other)
    : maximum_memory(other.maximum_memory.load()), block_allocator_size(other.block_allocator_size.load()),
      buffer_manager_track_eviction_timestamps(other.buffer_manager_track_eviction_timestamps),
      allocator_bulk_deallocation_flush_threshold(other.allocator_bulk_deallocation_flush_threshold.load()) {
}

DatabaseMemoryConfig &DatabaseMemoryConfig::operator=(const DatabaseMemoryConfig &other) {
	if (this == &other) {
		return *this;
	}
	maximum_memory = other.maximum_memory.load();
	block_allocator_size = other.block_allocator_size.load();
	buffer_manager_track_eviction_timestamps = other.buffer_manager_track_eviction_timestamps;
	allocator_bulk_deallocation_flush_threshold = other.allocator_bulk_deallocation_flush_threshold.load();
	return *this;
}

} // namespace duckdb
