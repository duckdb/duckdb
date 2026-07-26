//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_memory_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
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
	                      idx_t maximum_memory, bool track_eviction_timestamps,
	                      idx_t allocator_bulk_deallocation_flush_threshold);
	~DatabaseMemoryManager();

	DUCKDB_API static shared_ptr<DatabaseMemoryManager> Create(unique_ptr<DatabaseMemoryManagerOptions> options,
	                                                           DBConfig &config);

	DUCKDB_API Allocator &GetAllocator() const;
	DUCKDB_API BlockAllocator &GetBlockAllocator() const;
	DUCKDB_API TemporaryMemoryManager &GetTemporaryMemoryManager() const;
	DUCKDB_API BufferPool &GetBufferPool() const;
	DUCKDB_API ObjectCache &GetObjectCache() const;

private:
	unique_ptr<Allocator> allocator;
	unique_ptr<BlockAllocator> block_allocator;
	unique_ptr<TemporaryMemoryManager> temporary_memory_manager;
	unique_ptr<BufferPool> buffer_pool;
	unique_ptr<ObjectCache> object_cache;
};

} // namespace duckdb
