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
#include "duckdb/storage/buffer/managed_buffer.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {
class DatabaseInstance;
class TemporaryDirectoryHandle;
struct EvictionQueue;

//! The buffer manager is in charge of handling memory management for the database. It hands out memory buffers that can
//! be used by the database internally.
class BufferManager {
	friend class BufferHandle;
	friend class BlockHandle;

public:
	BufferManager(DatabaseInstance &db, string temp_directory, idx_t maximum_memory);
	~BufferManager();

	//! Register a block with the given block id in the base file
	shared_ptr<BlockHandle> RegisterBlock(block_id_t block_id);

	//! Register an in-memory buffer of arbitrary size, as long as it is >= BLOCK_SIZE. can_destroy signifies whether or
	//! not the buffer can be destroyed when unpinned, or whether or not it needs to be written to a temporary file so
	//! it can be reloaded. The resulting buffer will already be allocated, but needs to be pinned in order to be used.
	shared_ptr<BlockHandle> RegisterMemory(idx_t block_size, bool can_destroy);

	//! Convert an existing in-memory buffer into a persistent disk-backed block
	shared_ptr<BlockHandle> ConvertToPersistent(BlockManager &block_manager, block_id_t block_id,
	                                            shared_ptr<BlockHandle> old_block);

	//! Allocate an in-memory buffer with a single pin.
	//! The allocated memory is released when the buffer handle is destroyed.
	DUCKDB_API BufferHandle Allocate(idx_t block_size);

	//! Reallocate an in-memory buffer that is pinned.
	void ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size);

	BufferHandle Pin(shared_ptr<BlockHandle> &handle);
	void Unpin(shared_ptr<BlockHandle> &handle);

	void UnregisterBlock(block_id_t block_id, bool can_destroy);

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

private:
	//! Evict blocks until the currently used memory + extra_memory fit, returns false if this was not possible
	//! (i.e. not enough blocks could be evicted)
	//! If the "buffer" argument is specified AND the system can find a buffer to re-use for the given allocation size
	//! "buffer" will be made to point to the re-usable memory. Note that this is not guaranteed.
	bool EvictBlocks(idx_t extra_memory, idx_t memory_limit, unique_ptr<FileBuffer> *buffer = nullptr);

	//! Garbage collect eviction queue
	void PurgeQueue();

	//! Write a temporary buffer to disk
	void WriteTemporaryBuffer(ManagedBuffer &buffer);
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
	//! The lock for the set of blocks
	mutex blocks_lock;
	//! A mapping of block id -> BlockHandle
	unordered_map<block_id_t, weak_ptr<BlockHandle>> blocks;
	//! Eviction queue
	unique_ptr<EvictionQueue> queue;
	//! The temporary id used for managed buffers
	atomic<block_id_t> temporary_id;
	//! Allocator associated with the buffer manager, that passes all allocations through this buffer manager
	Allocator buffer_allocator;
};
} // namespace duckdb
