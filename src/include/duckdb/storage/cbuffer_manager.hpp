#pragma once

#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/storage/buffer/dummy_buffer_pool.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"

namespace duckdb {

class CBufferManager;
class ExternalFileBuffer;

typedef void *duckdb_block;

// Callbacks used by the CBufferManager

typedef duckdb_block (*duckdb_create_block_t)(void *data, idx_t size);
typedef duckdb_block (*duckdb_resize_block_t)(void *data, duckdb_block buffer, idx_t old_size, idx_t new_size);
typedef void (*duckdb_destroy_block_t)(void *data, duckdb_block buffer);
typedef void *(*duckdb_pin_block_t)(void *data, duckdb_block buffer);
typedef void (*duckdb_unpin_block_t)(void *data, duckdb_block buffer);
typedef idx_t (*duckdb_max_memory_t)(void *data);
typedef idx_t (*duckdb_used_memory_t)(void *data);

// Contains the information that makes up the custom buffer manager
struct CBufferManagerConfig {
	void *data; // Context provided to 'allocate_func'
	duckdb_create_block_t allocate_func;
	duckdb_resize_block_t reallocate_func;
	duckdb_destroy_block_t destroy_func;
	duckdb_pin_block_t pin_func;
	duckdb_unpin_block_t unpin_func;
	duckdb_max_memory_t max_memory_func;
	duckdb_used_memory_t used_memory_func;
};

class CustomInMemoryBlockManager : public InMemoryBlockManager {
public:
	using InMemoryBlockManager::InMemoryBlockManager;
public:
	shared_ptr<BlockHandle> RegisterBlock(block_id_t block_id, bool is_meta_block) final override;
};

class CBufferManager : public BufferManager {
public:
	CBufferManager(CBufferManagerConfig config);
	virtual ~CBufferManager() {
	}

public:
	BufferHandle Allocate(idx_t block_size, bool can_destroy = true,
	                      shared_ptr<BlockHandle> *block = nullptr) final override;
	void ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) final override;
	BufferHandle Pin(shared_ptr<BlockHandle> &handle) final override;
	void Unpin(shared_ptr<BlockHandle> &handle) final override;
	idx_t GetUsedMemory() const final override;
	void IncreaseUsedMemory(idx_t amount, bool unsafe = false) final override;
	void DecreaseUsedMemory(idx_t amount) final override;
	idx_t GetMaxMemory() const final override;
	Allocator &GetBufferAllocator() final override;
	shared_ptr<BlockHandle> RegisterSmallMemory(idx_t block_size) final override;
	BufferPool &GetBufferPool() final override;

protected:
	void PurgeQueue() final override;
	void DeleteTemporaryFile(block_id_t block_id) final override;
	void AddToEvictionQueue(shared_ptr<BlockHandle> &handle) final override;

private:
	// static data_ptr_t CBufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size);
	// static void CBufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);
	// static data_ptr_t CBufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
	//                                          idx_t size);

public:
	CBufferManagerConfig config;
	unique_ptr<BlockManager> block_manager;
	unique_ptr<DummyBufferPool> buffer_pool;
	//! The temporary id used for managed buffers
	atomic<block_id_t> temporary_id;
	// Allocator custom_allocator;
	Allocator allocator;
};

} // namespace duckdb
