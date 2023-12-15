//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/standard_buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/block_manager.hpp"

#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

namespace duckdb {
class BlockManager;
class DatabaseInstance;
class TemporaryDirectoryHandle;
struct EvictionQueue;

//! The BufferManager is in charge of handling memory management for a single database. It cooperatively shares a
//! BufferPool with other BufferManagers, belonging to different databases. It hands out memory buffers that can
//! be used by the database internally, and offers configuration options specific to a database, which need not be
//! shared by the BufferPool, including whether to support swapping temp buffers to disk, and where to swap them to.
class StandardBufferManager : public BufferManager {
	friend class BufferHandle;
	friend class BlockHandle;
	friend class BlockManager;

public:
	StandardBufferManager(DatabaseInstance &db, string temp_directory);
	virtual ~StandardBufferManager();

public:
	static unique_ptr<StandardBufferManager> CreateBufferManager(DatabaseInstance &db, string temp_directory);
	//! Registers an in-memory buffer that cannot be unloaded until it is destroyed
	//! This buffer can be small (smaller than BLOCK_SIZE)
	//! Unpin and pin are nops on this block of memory
	shared_ptr<BlockHandle> RegisterSmallMemory(idx_t block_size) final override;

	idx_t GetUsedMemory() const final override;
	idx_t GetMaxMemory() const final override;

	//! Allocate an in-memory buffer with a single pin.
	//! The allocated memory is released when the buffer handle is destroyed.
	DUCKDB_API BufferHandle Allocate(idx_t block_size, bool can_destroy = true,
	                                 shared_ptr<BlockHandle> *block = nullptr) final override;

	//! Reallocate an in-memory buffer that is pinned.
	void ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) final override;

	BufferHandle Pin(shared_ptr<BlockHandle> &handle) final override;
	void Unpin(shared_ptr<BlockHandle> &handle) final override;

	//! Set a new memory limit to the buffer manager, throws an exception if the new limit is too low and not enough
	//! blocks can be evicted
	void SetLimit(idx_t limit = (idx_t)-1) final override;

	//! Returns a list of all temporary files
	vector<TemporaryFileInformation> GetTemporaryFiles() final override;

	const string &GetTemporaryDirectory() final override {
		return temp_directory;
	}

	void SetTemporaryDirectory(const string &new_dir) final override;

	DUCKDB_API Allocator &GetBufferAllocator() final override;

	DatabaseInstance &GetDatabase() final override {
		return db;
	}

	//! Construct a managed buffer.
	unique_ptr<FileBuffer> ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&source,
	                                              FileBufferType type = FileBufferType::MANAGED_BUFFER) override;

	DUCKDB_API void ReserveMemory(idx_t size) final override;
	DUCKDB_API void FreeReservedMemory(idx_t size) final override;
	bool HasTemporaryDirectory() const final override;

protected:
	//! Helper
	template <typename... ARGS>
	TempBufferPoolReservation EvictBlocksOrThrow(idx_t memory_delta, unique_ptr<FileBuffer> *buffer, ARGS...);

	//! Register an in-memory buffer of arbitrary size, as long as it is >= BLOCK_SIZE. can_destroy signifies whether or
	//! not the buffer can be destroyed when unpinned, or whether or not it needs to be written to a temporary file so
	//! it can be reloaded. The resulting buffer will already be allocated, but needs to be pinned in order to be used.
	//! This needs to be private to prevent creating blocks without ever pinning them:
	//! blocks that are never pinned are never added to the eviction queue
	shared_ptr<BlockHandle> RegisterMemory(idx_t block_size, bool can_destroy);

	//! Evict blocks until the currently used memory + extra_memory fit, returns false if this was not possible
	//! (i.e. not enough blocks could be evicted)
	//! If the "buffer" argument is specified AND the system can find a buffer to re-use for the given allocation size
	//! "buffer" will be made to point to the re-usable memory. Note that this is not guaranteed.
	//! Returns a pair. result.first indicates if eviction was successful. result.second contains the
	//! reservation handle, which can be moved to the BlockHandle that will own the reservation.
	BufferPool::EvictionResult EvictBlocks(idx_t extra_memory, idx_t memory_limit,
	                                       unique_ptr<FileBuffer> *buffer = nullptr);

	//! Garbage collect eviction queue
	void PurgeQueue() final override;

	BufferPool &GetBufferPool() final override;

	//! Write a temporary buffer to disk
	void WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer) final override;
	//! Read a temporary buffer from disk
	unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> buffer = nullptr) final override;
	//! Get the path of the temporary buffer
	string GetTemporaryPath(block_id_t id);

	void DeleteTemporaryFile(block_id_t id) final override;

	void RequireTemporaryDirectory();

	void AddToEvictionQueue(shared_ptr<BlockHandle> &handle) final override;

	const char *InMemoryWarning();

	static data_ptr_t BufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size);
	static void BufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);
	static data_ptr_t BufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
	                                         idx_t size);

	//! When the BlockHandle reaches 0 readers, this creates a new FileBuffer for this BlockHandle and
	//! overwrites the data within with garbage. Any readers that do not hold the pin will notice
	void VerifyZeroReaders(shared_ptr<BlockHandle> &handle);

protected:
	//! The database instance
	DatabaseInstance &db;
	//! The buffer pool
	BufferPool &buffer_pool;
	//! The directory name where temporary files are stored
	string temp_directory;
	//! Lock for creating the temp handle
	mutex temp_handle_lock;
	//! Handle for the temporary directory
	unique_ptr<TemporaryDirectoryHandle> temp_directory_handle;
	//! The temporary id used for managed buffers
	atomic<block_id_t> temporary_id;
	//! Allocator associated with the buffer manager, that passes all allocations through this buffer manager
	Allocator buffer_allocator;
	//! Block manager for temp data
	unique_ptr<BlockManager> temp_block_manager;
};

} // namespace duckdb
