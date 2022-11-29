//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/virtual_buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/virtual_buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class Allocator;

class VirtualBufferManager {
	friend class BufferHandle;
	friend class BlockHandle;
	friend class BlockManager;

public:
	VirtualBufferManager(idx_t maximum_memory_p) : current_memory(0), maximum_memory(maximum_memory_p) {
	}
	virtual ~VirtualBufferManager() {
	}

public:
	virtual BufferHandle Allocate(idx_t block_size, bool can_destroy = true,
	                              shared_ptr<BlockHandle> *block = nullptr) = 0;
	//! FIXME: Maybe make this non-pure and just call Destroy and Allocate?
	virtual void ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) = 0;
	//! FIXME: Missing prototype for Destroy??
	virtual BufferHandle Pin(shared_ptr<BlockHandle> &handle) = 0;
	virtual void Unpin(shared_ptr<BlockHandle> &handle) = 0;
	virtual idx_t GetUsedMemory() {
		return current_memory;
	}
	virtual idx_t GetMaxMemory() {
		return maximum_memory;
	}
	virtual shared_ptr<BlockHandle> RegisterSmallMemory(idx_t block_size);
	virtual DUCKDB_API Allocator &GetBufferAllocator();
	virtual DUCKDB_API void ReserveMemory(idx_t size);
	virtual DUCKDB_API void FreeReservedMemory(idx_t size);
	virtual void SetLimit(idx_t limit = (idx_t)-1);
	virtual const string &GetTemporaryDirectory();
	virtual void SetTemporaryDirectory(string new_dir);
	virtual DatabaseInstance &GetDatabase();
	virtual bool HasTemporaryDirectory() const;
	virtual unique_ptr<FileBuffer> ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&source,
	                                                      FileBufferType type = FileBufferType::MANAGED_BUFFER);

	// Static methods

	static VirtualBufferManager &GetBufferManager(ClientContext &context);
	DUCKDB_API static VirtualBufferManager &GetBufferManager(DatabaseInstance &db);
	static idx_t GetAllocSize(idx_t block_size) {
		return AlignValue<idx_t, Storage::SECTOR_SIZE>(block_size + Storage::BLOCK_HEADER_SIZE);
	}

protected:
	virtual void AddToEvictionQueue(shared_ptr<BlockHandle> &handle);
	virtual void WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer);
	virtual unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> buffer);
	virtual void DeleteTemporaryFile(block_id_t id);

protected:
	//! The lock for changing the memory limit
	mutex limit_lock;
	//! The current amount of memory that is occupied by the buffer manager (in bytes)
	atomic<idx_t> current_memory;
	//! The maximum amount of memory that the buffer manager can keep (in bytes)
	atomic<idx_t> maximum_memory;
};

} // namespace duckdb
