// Should maybe be called ExternalBufferManager??

#pragma once

#include "duckdb/storage/virtual_buffer_manager.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {

class CBufferManager;

typedef void *duckdb_buffer;

// Callbacks used by the CBufferManager
typedef duckdb_buffer (*duckdb_allocate_buffer_t)(void *data, idx_t size);
typedef duckdb_buffer (*duckdb_reallocate_buffer_t)(duckdb_buffer buffer, idx_t old_size, idx_t new_size);
typedef void *(*duckdb_get_buffer_allocation_t)(duckdb_buffer buffer);
typedef void (*duckdb_destroy_buffer_t)(duckdb_buffer buffer);
typedef void (*duckdb_pin_buffer_t)(duckdb_buffer buffer);
typedef void (*duckdb_unpin_buffer_t)(duckdb_buffer buffer);
typedef idx_t (*duckdb_max_memory_t)(void *data);
typedef idx_t (*duckdb_used_memory_t)(void *data);

// Contains the information that makes up the virtual buffer manager
struct CBufferManagerConfig {
	void *data; // Context provided to 'allocate_func'
	duckdb_allocate_buffer_t allocate_func;
	duckdb_get_buffer_allocation_t get_allocation_func;
	duckdb_reallocate_buffer_t reallocate_func;
	duckdb_destroy_buffer_t destroy_func;
	duckdb_pin_buffer_t pin_func;
	duckdb_unpin_buffer_t unpin_func;
	duckdb_max_memory_t max_memory_func;
	duckdb_used_memory_t used_memory_func;
};

struct CBufferAllocatorData : public PrivateAllocatorData {
	CBufferAllocatorData(CBufferManager &manager) : manager(manager) {
	}
	//! User-provided data, provided to the 'allocate_func'
	CBufferManager &manager;
};

class CBufferManager : public VirtualBufferManager {
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
	void AdjustUsedMemory(int64_t amount) final override;
	idx_t GetMaxMemory() const final override;
	Allocator &GetBufferAllocator() final override;
	shared_ptr<BlockHandle> RegisterSmallMemory(idx_t block_size) final override;

protected:
	void PurgeQueue() final override;

private:
	static data_ptr_t CBufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size);
	static void CBufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);
	static data_ptr_t CBufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
	                                          idx_t size);

private:
	CBufferManagerConfig config;
	unique_ptr<BlockManager> block_manager;
	//! The temporary id used for managed buffers
	atomic<block_id_t> temporary_id;
	Allocator custom_allocator;
	Allocator allocator;
};

} // namespace duckdb
