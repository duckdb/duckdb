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

	//! Purge all blocks that haven't been pinned within the last N seconds
	idx_t PurgeAgedBlocks(uint32_t max_age_sec);

	//! Iterate over all purgable blocks and invoke the callback. If the callback returns true
	//! iteration continues.
	//! - Callback signature is: bool((BufferEvictionNode &, const std::shared_ptr<BlockHandle> &)
	//! - Callback is invoked while holding the corresponding BlockHandle mutex.
	template <typename FN>
	void IterateUnloadableBlocks(FN fn);

	//! Tries to dequeue an element from the eviction queue, but only after acquiring the purge queue lock.
	bool TryDequeueWithLock(BufferEvictionNode &node);
	//! Bulk purge dead nodes from the eviction queue. Then, enqueue those that are still alive.
	void PurgeIteration(const idx_t purge_size);
	//! Garbage collect dead nodes in the eviction queue.
	void PurgeQueue();
	//! Add a buffer handle to the eviction queue. Returns true, if the queue is
	//! ready to be purged, and false otherwise.
	bool AddToEvictionQueue(shared_ptr<BlockHandle> &handle);

	//! Increment the dead node counter in the purge queue.
	inline void IncrementDeadNodes() {
		total_dead_nodes++;
	}
	//! Decrement the dead node counter in the purge queue.
	inline void DecrementDeadNodes() {
		total_dead_nodes--;
	}

protected:
	//! The lock for changing the memory limit
	mutex limit_lock;
	//! The current amount of memory that is occupied by the buffer manager (in bytes)
	atomic<idx_t> current_memory;
	//! The maximum amount of memory that the buffer manager can keep (in bytes)
	atomic<idx_t> maximum_memory;
	//! Record timestamps of buffer manager unpin() events. Usable by custom eviction policies.
	bool track_eviction_timestamps;
	//! Eviction queue
	unique_ptr<EvictionQueue> queue;
	//! Memory manager for concurrently used temporary memory, e.g., for physical operators
	unique_ptr<TemporaryMemoryManager> temporary_memory_manager;
	//! Memory usage per tag
	atomic<idx_t> memory_usage_per_tag[MEMORY_TAG_COUNT];

	//! We trigger a purge of the eviction queue every INSERT_INTERVAL insertions
	constexpr static idx_t INSERT_INTERVAL = 4096;
	//! We multiply the base purge size by this value.
	constexpr static idx_t PURGE_SIZE_MULTIPLIER = 2;
	//! We multiply the purge size by this value to determine early-outs. This is the minimum queue size.
	//! We never purge below this point.
	constexpr static idx_t EARLY_OUT_MULTIPLIER = 4;
	//! We multiply the approximate alive nodes by this value to test whether our total dead nodes
	//! exceed their allowed ratio. Must be greater than 1.
	constexpr static idx_t ALIVE_NODE_MULTIPLIER = 4;

	//! Total number of insertions into the eviction queue. This guides the schedule for calling PurgeQueue.
	atomic<idx_t> evict_queue_insertions;
	//! Total dead nodes in the eviction queue. There are two scenarios in which a node dies: (1) we destroy its block
	//! handle, or (2) we insert a newer version into the eviction queue.
	atomic<idx_t> total_dead_nodes;
	//! Locked, if a queue purge is currently active or we're trying to forcefully evict a node.
	//! Only lets a single thread enter the purge phase.
	mutex purge_lock;

	//! A pre-allocated vector of eviction nodes. We reuse this to keep the allocation overhead of purges small.
	vector<BufferEvictionNode> purge_nodes;
};

} // namespace duckdb
