//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/buffer_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/common/array.hpp"

namespace duckdb {

class TemporaryMemoryManager;
struct EvictionQueue;

struct BufferEvictionNode {
	BufferEvictionNode() {
	}
	BufferEvictionNode(weak_ptr<BlockHandle> handle_p, idx_t timestamp_p)
	    : handle(std::move(handle_p)), timestamp(timestamp_p) {
		D_ASSERT(!handle.expired());
	}

	weak_ptr<BlockHandle> handle;
	idx_t timestamp;

	bool CanUnload(BlockHandle &handle_p);

	shared_ptr<BlockHandle> TryGetBlockHandle();
};

//! The BufferPool is in charge of handling memory management for one or more databases. It defines memory limits
//! and implements priority eviction among all users of the pool.
class BufferPool {
	friend class BlockHandle;
	friend class BlockManager;
	friend class BufferManager;
	friend class StandardBufferManager;

public:
	explicit BufferPool(idx_t maximum_memory);
	virtual ~BufferPool();

	//! Set a new memory limit to the buffer pool, throws an exception if the new limit is too low and not enough
	//! blocks can be evicted
	void SetLimit(idx_t limit, const char *exception_postscript);

	void IncreaseUsedMemory(MemoryTag tag, idx_t size);

	idx_t GetUsedMemory() const;

	idx_t GetMaxMemory() const;

	virtual idx_t GetQueryMaxMemory() const;

	TemporaryMemoryManager &GetTemporaryMemoryManager();

protected:
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
	virtual EvictionResult EvictBlocks(MemoryTag tag, idx_t extra_memory, idx_t memory_limit,
	                                   unique_ptr<FileBuffer> *buffer = nullptr);

	//! Garbage collect eviction queue
	void PurgeQueue();
	idx_t PurgeIteration();
	void AddToEvictionQueue(shared_ptr<BlockHandle> &handle);

protected:
	//! The lock for changing the memory limit
	mutex limit_lock;
	//! The current amount of memory that is occupied by the buffer manager (in bytes)
	atomic<idx_t> current_memory;
	//! The maximum amount of memory that the buffer manager can keep (in bytes)
	atomic<idx_t> maximum_memory;
	//! Eviction queue
	unique_ptr<EvictionQueue> queue;
	//! Memory manager for concurrently used temporary memory, e.g., for physical operators
	unique_ptr<TemporaryMemoryManager> temporary_memory_manager;
	//! Memory usage per tag
	atomic<idx_t> memory_usage_per_tag[MEMORY_TAG_COUNT];

	//! We trigger a purge of the eviction queue every INSERT_INTERVAL insertions
	constexpr static idx_t INSERT_INTERVAL = 1024;
	//!
	constexpr static float ACTIVE_THRESHOLD = 0.4;
	//! Initial maximum queue size - this is an adaptive ceiling. If we purge the entire queue, and we
	//! encounter more than 50% active nodes, we increase the ceiling.
	idx_t max_queue_size = 8 * INSERT_INTERVAL;

	//! Defines how many nodes we attempt to purge per bulk purge iteration
	constexpr static idx_t BULK_PURGE_SIZE = 256;

	//! Total number of insertions into the eviction queue. This guides the schedule for calling PurgeQueue.
	atomic<idx_t> evict_queue_insertions;
	//! Whether a queue purge is currently active
	atomic<bool> purge_active;
	//! The maximum number of nodes that we purge per bulk purge iteration
	array<BufferEvictionNode, BULK_PURGE_SIZE> purge_nodes;
};

} // namespace duckdb
