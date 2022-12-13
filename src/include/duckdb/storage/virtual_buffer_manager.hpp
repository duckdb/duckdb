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
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class Allocator;

class VirtualBufferManager {
	friend class BufferHandle;
	friend class BlockHandle;
	friend class BlockManager;

public:
	VirtualBufferManager() {
	}
	virtual ~VirtualBufferManager() {
	}

public:
	virtual BufferHandle Allocate(idx_t block_size, bool can_destroy = true,
	                              shared_ptr<BlockHandle> *block = nullptr) = 0;
	virtual void ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) = 0;
	virtual BufferHandle Pin(shared_ptr<BlockHandle> &handle) = 0;
	virtual void Unpin(shared_ptr<BlockHandle> &handle) = 0;
	virtual idx_t GetUsedMemory() const = 0;
	virtual idx_t GetMaxMemory() const = 0;
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

	virtual void AdjustUsedMemory(int64_t amount) = 0;
	static VirtualBufferManager &GetBufferManager(ClientContext &context);
	DUCKDB_API static VirtualBufferManager &GetBufferManager(DatabaseInstance &db);
	static idx_t GetAllocSize(idx_t block_size) {
		return AlignValue<idx_t, Storage::SECTOR_SIZE>(block_size + Storage::BLOCK_HEADER_SIZE);
	}

protected:
	virtual void PurgeQueue() = 0;
	virtual void AddToEvictionQueue(shared_ptr<BlockHandle> &handle);
	virtual void WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer);
	virtual unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> buffer);
	virtual void DeleteTemporaryFile(block_id_t id);
};

} // namespace duckdb
