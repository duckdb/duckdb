//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer/temporary_file_information.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/file_buffer.hpp"

namespace duckdb {

class Allocator;
class BufferPool;
class BlockManager;

//! The BufferManager is in charge of handling memory management for a single database. It cooperatively shares a
//! BufferPool with other BufferManagers, belonging to different databases. It hands out memory buffers that can
//! be used by the database internally, and offers configuration options specific to a database, which need not be
//! shared by the BufferPool, including whether to support swapping temp buffers to disk, and where to swap them to.
class BufferManager {
	friend class BufferHandle;
	friend class BlockHandle;
	friend class BlockManager;

public:
	BufferManager();
	virtual ~BufferManager();
	BufferManager(const BufferManager &other) = delete;
	BufferManager(const BufferManager &&other) = delete;

public:
	static unique_ptr<BufferManager> CreateStandardBufferManager(DatabaseInstance &db, DBConfig &config);
	virtual BufferHandle Allocate(idx_t block_size, bool can_destroy = true,
	                              shared_ptr<BlockHandle> *block = nullptr) = 0;
	//! Reallocate an in-memory buffer that is pinned.
	virtual void ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) = 0;
	virtual BufferHandle Pin(shared_ptr<BlockHandle> &handle) = 0;
	virtual void Unpin(shared_ptr<BlockHandle> &handle) = 0;
	//! Returns the currently allocated memory
	virtual idx_t GetUsedMemory() const = 0;
	//! Returns the maximum available memory
	virtual idx_t GetMaxMemory() const = 0;
	virtual shared_ptr<BlockHandle> RegisterSmallMemory(idx_t block_size);
	virtual DUCKDB_API Allocator &GetBufferAllocator();
	virtual DUCKDB_API void ReserveMemory(idx_t size);
	virtual DUCKDB_API void FreeReservedMemory(idx_t size);
	//! Set a new memory limit to the buffer manager, throws an exception if the new limit is too low and not enough
	//! blocks can be evicted
	virtual void SetLimit(idx_t limit = (idx_t)-1);
	virtual vector<TemporaryFileInformation> GetTemporaryFiles();
	virtual const string &GetTemporaryDirectory();
	virtual void SetTemporaryDirectory(const string &new_dir);
	virtual DatabaseInstance &GetDatabase();
	virtual bool HasTemporaryDirectory() const;
	//! Construct a managed buffer.
	virtual unique_ptr<FileBuffer> ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&source,
	                                                      FileBufferType type = FileBufferType::MANAGED_BUFFER);
	//! Increases the currently allocated memory, but the actual allocation does not go through the buffer manager
	virtual void IncreaseUsedMemory(idx_t size, bool unsafe = false) = 0;
	//! Decrease the currently allocated memory, but the actual deallocation does not go through the buffer manager
	virtual void DecreaseUsedMemory(idx_t size) = 0;
	//! Get the underlying buffer pool responsible for managing the buffers
	virtual BufferPool &GetBufferPool();

	// Static methods

	DUCKDB_API static BufferManager &GetBufferManager(DatabaseInstance &db);
	DUCKDB_API static BufferManager &GetBufferManager(ClientContext &context);
	DUCKDB_API static BufferManager &GetBufferManager(AttachedDatabase &db);

	static idx_t GetAllocSize(idx_t block_size);

protected:
	virtual void PurgeQueue() = 0;
	virtual void AddToEvictionQueue(shared_ptr<BlockHandle> &handle);
	virtual void WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer);
	virtual unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> buffer);
	virtual void DeleteTemporaryFile(block_id_t id);
};

} // namespace duckdb
