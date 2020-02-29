//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer/buffer_list.hpp"
#include "duckdb/storage/buffer/managed_buffer.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <mutex>

namespace duckdb {

//! The buffer manager is in charge of handling memory management for the database. It hands out memory buffers that can
//! be used by the database internally.
class BufferManager {
	friend class BufferHandle;

public:
	BufferManager(FileSystem &fs, BlockManager &manager, string temp_directory, idx_t maximum_memory);
	~BufferManager();

	//! Pin a block id, returning a block handle holding a pointer to the block
	unique_ptr<BufferHandle> Pin(block_id_t block, bool can_destroy = false);

	//! Allocate a buffer of arbitrary size, as long as it is >= BLOCK_SIZE. can_destroy signifies whether or not the
	//! buffer can be destroyed when unpinned, or whether or not it needs to be written to a temporary file so it can be
	//! reloaded.
	unique_ptr<BufferHandle> Allocate(idx_t alloc_size, bool can_destroy = false);
	//! Destroy the managed buffer with the specified buffer_id, freeing its memory
	void DestroyBuffer(block_id_t buffer_id, bool can_destroy = false);

	//! Set a new memory limit to the buffer manager, throws an exception if the new limit is too low and not enough
	//! blocks can be evicted
	void SetLimit(idx_t limit = (idx_t)-1);

	static BufferManager &GetBufferManager(ClientContext &context);

private:
	unique_ptr<BufferHandle> PinBlock(block_id_t block_id);
	unique_ptr<BufferHandle> PinBuffer(block_id_t block_id, bool can_destroy = false);

	//! Unpin a block id, decreasing its reference count and potentially allowing it to be freed.
	void Unpin(block_id_t block);

	//! Evict the least recently used block from the buffer manager, or throws an exception if there are no blocks
	//! available to evict
	unique_ptr<Block> EvictBlock();

	//! Add a reference to the refcount of a buffer entry
	void AddReference(BufferEntry *entry);

	//! Write a temporary buffer to disk
	void WriteTemporaryBuffer(ManagedBuffer &buffer);
	//! Read a temporary buffer from disk
	unique_ptr<BufferHandle> ReadTemporaryBuffer(block_id_t id);
	//! Get the path of the temporary buffer
	string GetTemporaryPath(block_id_t id);

	void DeleteTemporaryFile(block_id_t id);

private:
	FileSystem &fs;
	//! The block manager
	BlockManager &manager;
	//! The current amount of memory that is occupied by the buffer manager (in bytes)
	idx_t current_memory;
	//! The maximum amount of memory that the buffer manager can keep (in bytes)
	idx_t maximum_memory;
	//! The directory name where temporary files are stored
	string temp_directory;
	//! The lock for the set of blocks
	std::mutex block_lock;
	//! A mapping of block id -> BufferEntry
	unordered_map<block_id_t, BufferEntry *> blocks;
	//! A linked list of buffer entries that are in use
	BufferList used_list;
	//! LRU list of unused blocks
	BufferList lru;
	//! The temporary id used for managed buffers
	block_id_t temporary_id;
};
} // namespace duckdb
