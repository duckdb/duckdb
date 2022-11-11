//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {
class BlockManager;
class DatabaseInstance;
class TemporaryDirectoryHandle;
struct EvictionQueue;

//! The buffer manager is in charge of handling memory management for the database. It hands out memory buffers that can
//! be used by the database internally.
//
//! BlockIds are NOT unique within the context of a BufferManager. A buffer manager
//! can be shared by many BlockManagers.
class BufferManager {
	friend class BufferHandle;
	friend class BlockHandle;
	friend class BlockManager;

public:
	BufferManager(DatabaseInstance &db, string temp_directory, idx_t maximum_memory);
	virtual ~BufferManager();

	//! Register an in-memory buffer of arbitrary size, as long as it is >= BLOCK_SIZE. can_destroy signifies whether or
	//! not the buffer can be destroyed when unpinned, or whether or not it needs to be written to a temporary file so
	//! it can be reloaded. The resulting buffer will already be allocated, but needs to be pinned in order to be used.
	shared_ptr<BlockHandle> RegisterMemory(idx_t block_size, bool can_destroy);
	//! Registers an in-memory buffer that cannot be unloaded until it is destroyed
	//! This buffer can be small (smaller than BLOCK_SIZE)
	//! Unpin and pin are nops on this block of memory
	shared_ptr<BlockHandle> RegisterSmallMemory(idx_t block_size);

	//! Allocate an in-memory buffer with a single pin.
	//! The allocated memory is released when the buffer handle is destroyed.
	DUCKDB_API BufferHandle Allocate(idx_t block_size);

	//! Reallocate an in-memory buffer that is pinned.
	void ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size);

	BufferHandle Pin(shared_ptr<BlockHandle> &handle);
	void Unpin(shared_ptr<BlockHandle> &handle);

	//! Set a new memory limit to the buffer manager, throws an exception if the new limit is too low and not enough
	//! blocks can be evicted
	void SetLimit(idx_t limit = (idx_t)-1);

	static BufferManager &GetBufferManager(ClientContext &context);
	DUCKDB_API static BufferManager &GetBufferManager(DatabaseInstance &db);

	idx_t GetUsedMemory() {
		return current_memory;
	}
	idx_t GetMaxMemory() {
		return maximum_memory;
	}

	const string &GetTemporaryDirectory() {
		return temp_directory;
	}

	void SetTemporaryDirectory(string new_dir);

	DUCKDB_API Allocator &GetBufferAllocator();

	DatabaseInstance &GetDatabase() {
		return db;
	}

	//! Construct a managed buffer.
	//! The block_id is just used for internal tracking. It doesn't map to any actual
	//! BlockManager.
	virtual unique_ptr<FileBuffer> ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&source,
	                                                      FileBufferType type = FileBufferType::MANAGED_BUFFER);

	DUCKDB_API void ReserveMemory(idx_t size);
	DUCKDB_API void FreeReservedMemory(idx_t size);

private:
	//! Evict blocks until the currently used memory + extra_memory fit, returns false if this was not possible
	//! (i.e. not enough blocks could be evicted)
	//! If the "buffer" argument is specified AND the system can find a buffer to re-use for the given allocation size
	//! "buffer" will be made to point to the re-usable memory. Note that this is not guaranteed.
	//! Returns a pair. result.first indicates if eviction was successful. result.second contains the
	//! reservation handle, which can be moved to the BlockHandle that will own the reservation.
	struct EvictionResult {
		bool success;
		TempBufferPoolReservation reservation;
	};
	EvictionResult EvictBlocks(idx_t extra_memory, idx_t memory_limit, unique_ptr<FileBuffer> *buffer = nullptr);

	//! Helper
	template <typename... ARGS>
	TempBufferPoolReservation EvictBlocksOrThrow(idx_t extra_memory, idx_t limit, unique_ptr<FileBuffer> *buffer,
	                                             ARGS...);

	//! Garbage collect eviction queue
	void PurgeQueue();

	//! Write a temporary buffer to disk
	void WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer);
	//! Read a temporary buffer from disk
	unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> buffer = nullptr);
	//! Get the path of the temporary buffer
	string GetTemporaryPath(block_id_t id);

	void DeleteTemporaryFile(block_id_t id);

	void RequireTemporaryDirectory();

	void AddToEvictionQueue(shared_ptr<BlockHandle> &handle);

	string InMemoryWarning();

	static data_ptr_t BufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size);
	static void BufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);
	static data_ptr_t BufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
	                                         idx_t size);

private:
	//! The database instance
	DatabaseInstance &db;
	//! The lock for changing the memory limit
	mutex limit_lock;
	//! The current amount of memory that is occupied by the buffer manager (in bytes)
	atomic<idx_t> current_memory;
	//! The maximum amount of memory that the buffer manager can keep (in bytes)
	atomic<idx_t> maximum_memory;
	//! The directory name where temporary files are stored
	string temp_directory;
	//! Lock for creating the temp handle
	mutex temp_handle_lock;
	//! Handle for the temporary directory
	unique_ptr<TemporaryDirectoryHandle> temp_directory_handle;
	//! Eviction queue
	unique_ptr<EvictionQueue> queue;
	//! The temporary id used for managed buffers
	atomic<block_id_t> temporary_id;
	//! Total number of insertions into the eviction queue. This guides the schedule for calling PurgeQueue.
	atomic<uint32_t> queue_insertions;
	//! Allocator associated with the buffer manager, that passes all allocations through this buffer manager
	Allocator buffer_allocator;
	//! Block manager for temp data
	unique_ptr<BlockManager> temp_block_manager;
};

} // namespace duckdb
