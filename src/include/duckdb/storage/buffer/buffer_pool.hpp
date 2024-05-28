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

namespace duckdb {

class TemporaryMemoryManager;
struct EvictionQueue;

struct BufferEvictionNode {
	BufferEvictionNode() {
	}
	BufferEvictionNode(weak_ptr<BlockHandle> handle_p, idx_t eviction_seq_num);

	weak_ptr<BlockHandle> handle;
	idx_t handle_sequence_number;

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
	explicit BufferPool(idx_t maximum_memory, bool track_eviction_timestamps);
	virtual ~BufferPool();

	//! Set a new memory limit to the buffer pool, throws an exception if the new limit is too low and not enough
	//! blocks can be evicted
	void SetLimit(idx_t limit, const char *exception_postscript);

	void UpdateUsedMemory(MemoryTag tag, int64_t size);

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
	virtual EvictionResult EvictBlocksInternal(EvictionQueue &queue, MemoryTag tag, idx_t extra_memory,
	                                           idx_t memory_limit, unique_ptr<FileBuffer> *buffer = nullptr);

	//! Purge all blocks that haven't been pinned within the last N seconds
	idx_t PurgeAgedBlocks(uint32_t max_age_sec);
	idx_t PurgeAgedBlocksInternal(EvictionQueue &queue, uint32_t max_age_sec, int64_t now, int64_t limit);
	//! Garbage collect dead nodes in the eviction queue.
	void PurgeQueue(FileBufferType type);
	//! Add a buffer handle to the eviction queue. Returns true, if the queue is
	//! ready to be purged, and false otherwise.
	bool AddToEvictionQueue(shared_ptr<BlockHandle> &handle);
	//! Gets the eviction queue for the specified type
	EvictionQueue &GetEvictionQueueForType(FileBufferType type);
	//! Increments the dead nodes for the queue with specified type
	void IncrementDeadNodes(FileBufferType type);

protected:
	//! The lock for changing the memory limit
	mutex limit_lock;
	//! The current amount of memory that is occupied by the buffer manager (in bytes)
	atomic<idx_t> current_memory;
	//! The maximum amount of memory that the buffer manager can keep (in bytes)
	atomic<idx_t> maximum_memory;
	//! Record timestamps of buffer manager unpin() events. Usable by custom eviction policies.
	bool track_eviction_timestamps;
	//! Eviction queues
	vector<unique_ptr<EvictionQueue>> queues;
	//! Memory manager for concurrently used temporary memory, e.g., for physical operators
	unique_ptr<TemporaryMemoryManager> temporary_memory_manager;
	//! Memory usage per tag
	atomic<idx_t> memory_usage_per_tag[MEMORY_TAG_COUNT];
};

} // namespace duckdb
