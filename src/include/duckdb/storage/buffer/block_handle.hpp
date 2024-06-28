//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/block_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/enums/memory_tag.hpp"

namespace duckdb {
class BlockManager;
class BufferHandle;
class BufferPool;
class DatabaseInstance;

enum class BlockState : uint8_t { BLOCK_UNLOADED = 0, BLOCK_LOADED = 1 };

struct BufferPoolReservation {
	MemoryTag tag;
	idx_t size {0};
	BufferPool &pool;

	BufferPoolReservation(MemoryTag tag, BufferPool &pool);
	BufferPoolReservation(const BufferPoolReservation &) = delete;
	BufferPoolReservation &operator=(const BufferPoolReservation &) = delete;

	BufferPoolReservation(BufferPoolReservation &&) noexcept;
	BufferPoolReservation &operator=(BufferPoolReservation &&) noexcept;

	~BufferPoolReservation();

	void Resize(idx_t new_size);
	void Merge(BufferPoolReservation src);
};

struct TempBufferPoolReservation : BufferPoolReservation {
	TempBufferPoolReservation(MemoryTag tag, BufferPool &pool, idx_t size) : BufferPoolReservation(tag, pool) {
		Resize(size);
	}
	TempBufferPoolReservation(TempBufferPoolReservation &&) = default;
	~TempBufferPoolReservation() {
		Resize(0);
	}
};

class BlockHandle {
	friend class BlockManager;
	friend struct BufferEvictionNode;
	friend class BufferHandle;
	friend class BufferManager;
	friend class StandardBufferManager;
	friend class BufferPool;
	friend struct EvictionQueue;

public:
	BlockHandle(BlockManager &block_manager, block_id_t block_id, MemoryTag tag);
	BlockHandle(BlockManager &block_manager, block_id_t block_id, MemoryTag tag, unique_ptr<FileBuffer> buffer,
	            bool can_destroy, idx_t block_size, BufferPoolReservation &&reservation);
	~BlockHandle();

	BlockManager &block_manager;

public:
	block_id_t BlockId() {
		return block_id;
	}

	void ResizeBuffer(idx_t block_size, int64_t memory_delta) {
		D_ASSERT(buffer);
		// resize and adjust current memory
		buffer->Resize(block_size);
		memory_usage = NumericCast<idx_t>(NumericCast<int64_t>(memory_usage) + memory_delta);
		D_ASSERT(memory_usage == buffer->AllocSize());
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

	inline void SetCanDestroy(bool can_destroy_p) {
		can_destroy = can_destroy_p;
	}

	inline const idx_t &GetMemoryUsage() const {
		return memory_usage;
	}
	bool IsUnloaded() {
		return state == BlockState::BLOCK_UNLOADED;
	}

private:
	static BufferHandle Load(shared_ptr<BlockHandle> &handle, unique_ptr<FileBuffer> buffer = nullptr);
	static BufferHandle LoadFromBuffer(shared_ptr<BlockHandle> &handle, data_ptr_t data,
	                                   unique_ptr<FileBuffer> reusable_buffer);
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
	//! Memory tag
	MemoryTag tag;
	//! Pointer to loaded data (if any)
	unique_ptr<FileBuffer> buffer;
	//! Internal eviction sequence number
	atomic<idx_t> eviction_seq_num;
	//! LRU timestamp (for age-based eviction)
	atomic<int64_t> lru_timestamp_msec;
	//! Whether or not the buffer can be destroyed (only used for temporary buffers)
	bool can_destroy;
	//! The memory usage of the block (when loaded). If we are pinning/loading
	//! an unloaded block, this tells us how much memory to reserve.
	idx_t memory_usage;
	//! Current memory reservation / usage
	BufferPoolReservation memory_charge;
	//! Does the block contain any memory pointers?
	const char *unswizzled;
};

} // namespace duckdb
