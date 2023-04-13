//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer/temporary_file_information.hpp"

namespace duckdb {
class BlockManager;
class DatabaseInstance;
class TemporaryDirectoryHandle;
struct EvictionQueue;

//! The BufferManager is in charge of handling memory management for a single database. It cooperatively shares a
//! BufferPool with other BufferManagers, belonging to different databases. It hands out memory buffers that can
//! be used by the database internally, and offers configuration options specific to a database, which need not be
//! shared by the BufferPool, including whether to support swapping temp buffers to disk, and where to swap them to.
class BufferManager {
	friend class BufferHandle;
	friend class BlockHandle;
	friend class BlockManager;

public:
	BufferManager(DatabaseInstance &db, string temp_directory);
	virtual ~BufferManager();

	//! Registers an in-memory buffer that cannot be unloaded until it is destroyed
	//! This buffer can be small (smaller than BLOCK_SIZE)
	//! Unpin and pin are nops on this block of memory
	shared_ptr<BlockHandle> RegisterSmallMemory(idx_t block_size);

	//! Allocate an in-memory buffer with a single pin.
	//! The allocated memory is released when the buffer handle is destroyed.
	DUCKDB_API BufferHandle Allocate(idx_t block_size, bool can_destroy = true,
	                                 shared_ptr<BlockHandle> *block = nullptr);

	//! Reallocate an in-memory buffer that is pinned.
	void ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size);

	BufferHandle Pin(shared_ptr<BlockHandle> &handle);
	void Unpin(shared_ptr<BlockHandle> &handle);

	DUCKDB_API static BufferManager &GetBufferManager(ClientContext &context);
	DUCKDB_API static BufferManager &GetBufferManager(DatabaseInstance &db);
	DUCKDB_API static BufferManager &GetBufferManager(AttachedDatabase &db);

	//! Returns the currently allocated memory
	idx_t GetUsedMemory() {
		return buffer_pool.current_memory;
	}
	//! Returns the maximum available memory
	idx_t GetMaxMemory() {
		return buffer_pool.maximum_memory;
	}

	//! Increases the currently allocated memory, but the actual allocation does not go through the buffer manager
	void IncreaseUsedMemory(idx_t size);
	//! Decrease the currently allocated memory, but the actual deallocation does not go through the buffer manager
	void DecreaseUsedMemory(idx_t size);

	const string &GetTemporaryDirectory() {
		return temp_directory;
	}

	void SetTemporaryDirectory(string new_dir);

	DUCKDB_API Allocator &GetBufferAllocator();

	DatabaseInstance &GetDatabase() {
		return db;
	}

	BufferPool &GetBufferPool() {
		return buffer_pool;
	}

	static idx_t GetAllocSize(idx_t block_size) {
		return AlignValue<idx_t, Storage::SECTOR_SIZE>(block_size + Storage::BLOCK_HEADER_SIZE);
	}

	//! Construct a managed buffer.
	unique_ptr<FileBuffer> ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&source,
	                                              FileBufferType type = FileBufferType::MANAGED_BUFFER);

	DUCKDB_API void ReserveMemory(idx_t size);
	DUCKDB_API void FreeReservedMemory(idx_t size);

	//! Set a new memory limit to the buffer pool, throws an exception if the new limit is too low and not enough
	//! blocks can be evicted. (Sugar for calling method directly on the BufferPool.)
	void SetLimit(idx_t limit = (idx_t)-1) {
		buffer_pool.SetLimit(limit, InMemoryWarning());
	}
	//! Returns a list of all temporary files
	vector<TemporaryFileInformation> GetTemporaryFiles();

private:
	//! Register an in-memory buffer of arbitrary size, as long as it is >= BLOCK_SIZE. can_destroy signifies whether or
	//! not the buffer can be destroyed when unpinned, or whether or not it needs to be written to a temporary file so
	//! it can be reloaded. The resulting buffer will already be allocated, but needs to be pinned in order to be used.
	//! This needs to be private to prevent creating blocks without ever pinning them:
	//! blocks that are never pinned are never added to the eviction queue
	shared_ptr<BlockHandle> RegisterMemory(idx_t block_size, bool can_destroy);

	//! Write a temporary buffer to disk
	void WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer);
	//! Read a temporary buffer from disk
	unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> buffer = nullptr);
	//! Get the path of the temporary buffer
	string GetTemporaryPath(block_id_t id);

	void DeleteTemporaryFile(block_id_t id);

	void RequireTemporaryDirectory();

	const char *InMemoryWarning();

	static data_ptr_t BufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size);
	static void BufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);
	static data_ptr_t BufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
	                                         idx_t size);

	//! When the BlockHandle reaches 0 readers, this creates a new FileBuffer for this BlockHandle and
	//! overwrites the data within with garbage. Any readers that do not hold the pin will notice
	void VerifyZeroReaders(shared_ptr<BlockHandle> &handle);

	//! Helper
	template <typename... ARGS>
	TempBufferPoolReservation EvictBlocksOrThrow(idx_t extra_memory, unique_ptr<FileBuffer> *buffer, ARGS...);

private:
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
