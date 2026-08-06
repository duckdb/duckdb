//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_memory_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/database_memory_config.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/storage/block_allocator.hpp"

namespace duckdb {

class BufferPool;
struct DBConfig;
class TemporaryMemoryManager;
class ObjectCache;

//! Initialization-only ownership transferred into a DatabaseMemoryManager.
struct DatabaseMemoryManagerOptions {
	DatabaseMemoryManagerOptions() = default;
	~DatabaseMemoryManagerOptions() = default;

	unique_ptr<Allocator> allocator;
	unique_ptr<BlockAllocator> block_allocator;
};

//! Owns the memory-management components shared by one or more DatabaseInstances.
class DatabaseMemoryManager {
public:
	DatabaseMemoryManager(unique_ptr<Allocator> allocator, unique_ptr<BlockAllocator> block_allocator,
	                      const DatabaseMemoryConfig &config);
	~DatabaseMemoryManager();

	DUCKDB_API static shared_ptr<DatabaseMemoryManager> Create(unique_ptr<DatabaseMemoryManagerOptions> options,
	                                                           DBConfig &config);

	DUCKDB_API Allocator &GetAllocator() const;
	DUCKDB_API BlockAllocator &GetBlockAllocator() const;
	DUCKDB_API TemporaryMemoryManager &GetTemporaryMemoryManager() const;
	DUCKDB_API BufferPool &GetBufferPool() const;
	ObjectCache &GetSharedObjectCache() const;
	DUCKDB_API const DatabaseMemoryConfig &GetConfig() const;
	DUCKDB_API void SetMaximumMemory(idx_t maximum_memory, const char *exception_postscript);
	DUCKDB_API void SetBlockAllocatorSize(idx_t block_allocator_size);
	DUCKDB_API void SetAllocatorBulkDeallocationFlushThreshold(idx_t threshold);

private:
	DatabaseMemoryConfig config;
	unique_ptr<Allocator> allocator;
	unique_ptr<BlockAllocator> block_allocator;
	unique_ptr<TemporaryMemoryManager> temporary_memory_manager;
	unique_ptr<BufferPool> buffer_pool;
	unique_ptr<ObjectCache> object_cache;
};

} // namespace duckdb
