//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/block_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {
class BlockManager;
class BufferHandle;
class BufferManager;
class DatabaseInstance;
class FileBuffer;

enum class BlockState : uint8_t { BLOCK_UNLOADED = 0, BLOCK_LOADED = 1 };

class BlockHandle {
	friend class BlockManager;
	friend struct BufferEvictionNode;
	friend class BufferHandle;
	friend class BufferManager;

public:
	BlockHandle(BlockManager &block_manager, block_id_t block_id);
	BlockHandle(BlockManager &block_manager, block_id_t block_id, unique_ptr<FileBuffer> buffer, bool can_destroy,
	            idx_t block_size);
	~BlockHandle();

	BlockManager &block_manager;

public:
	block_id_t BlockId() {
		return block_id;
	}

	int32_t Readers() const {
		return readers;
	}

	inline bool IsSwizzled() const {
		return !unswizzled;
	}

	inline void SetSwizzling(const char *unswizzler) {
		unswizzled = unswizzler;
	}

private:
	static BufferHandle Load(shared_ptr<BlockHandle> &handle, unique_ptr<FileBuffer> buffer = nullptr);
	unique_ptr<FileBuffer> UnloadAndTakeBlock();
	void Unload();
	bool CanUnload();

	//! The block-level lock
	mutex lock;
	//! Whether or not the block is loaded/unloaded
	atomic<BlockState> state;
	//! Amount of concurrent readers
	atomic<int32_t> readers;
	//! The block id of the block
	const block_id_t block_id;
	//! Pointer to loaded data (if any)
	unique_ptr<FileBuffer> buffer;
	//! Internal eviction timestamp
	atomic<idx_t> eviction_timestamp;
	//! Whether or not the buffer can be destroyed (only used for temporary buffers)
	const bool can_destroy;
	//! The memory usage of the block
	idx_t memory_usage;
	//! Does the block contain any memory pointers?
	const char *unswizzled;
};

} // namespace duckdb
