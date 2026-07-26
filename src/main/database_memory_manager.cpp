#include "duckdb/main/database_memory_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

namespace duckdb {

DatabaseMemoryManager::DatabaseMemoryManager(unique_ptr<Allocator> allocator_p,
                                             unique_ptr<BlockAllocator> block_allocator_p, idx_t maximum_memory,
                                             bool track_eviction_timestamps,
                                             idx_t allocator_bulk_deallocation_flush_threshold)
    : allocator(std::move(allocator_p)), block_allocator(std::move(block_allocator_p)) {
	if (!allocator || !block_allocator) {
		throw InternalException("DatabaseMemoryManager cannot contain null allocator components");
	}
	if (&block_allocator->GetAllocator() != allocator.get()) {
		throw InternalException("DatabaseMemoryManager allocator components belong to different memory domains");
	}
	temporary_memory_manager = make_uniq<TemporaryMemoryManager>();
	buffer_pool = make_uniq<BufferPool>(*block_allocator, *temporary_memory_manager, maximum_memory,
	                                    track_eviction_timestamps, allocator_bulk_deallocation_flush_threshold);
	object_cache = make_uniq<ObjectCache>(*buffer_pool);
	buffer_pool->RegisterObjectCache(*object_cache);
}

DatabaseMemoryManager::~DatabaseMemoryManager() {
	buffer_pool->UnregisterObjectCache(*object_cache);
	block_allocator->FlushAll();
	Allocator::SetBackgroundThreads(false);
}

shared_ptr<DatabaseMemoryManager> DatabaseMemoryManager::Create(unique_ptr<DatabaseMemoryManagerOptions> options,
                                                                DBConfig &config) {
	unique_ptr<Allocator> allocator;
	unique_ptr<BlockAllocator> block_allocator;
	if (options) {
		allocator = std::move(options->allocator);
		block_allocator = std::move(options->block_allocator);
	}
	if (!allocator) {
		allocator = make_uniq<Allocator>();
	}
	if (!block_allocator) {
		auto default_block_size = Settings::Get<DefaultBlockSizeSetting>(config);
		block_allocator = make_uniq<BlockAllocator>(*allocator, default_block_size,
		                                            DBConfig::GetSystemAvailableMemory(*config.file_system) * 8 / 10,
		                                            config.options.block_allocator_size);
	}
	return make_shared_ptr<DatabaseMemoryManager>(std::move(allocator), std::move(block_allocator),
	                                              config.options.maximum_memory,
	                                              config.options.buffer_manager_track_eviction_timestamps,
	                                              config.options.allocator_bulk_deallocation_flush_threshold);
}

Allocator &DatabaseMemoryManager::GetAllocator() const {
	return *allocator;
}

BlockAllocator &DatabaseMemoryManager::GetBlockAllocator() const {
	return *block_allocator;
}

TemporaryMemoryManager &DatabaseMemoryManager::GetTemporaryMemoryManager() const {
	return *temporary_memory_manager;
}

BufferPool &DatabaseMemoryManager::GetBufferPool() const {
	return *buffer_pool;
}

ObjectCache &DatabaseMemoryManager::GetObjectCache() const {
	return *object_cache;
}

} // namespace duckdb
