#include "duckdb/storage/buffer/buffer_pool.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

namespace duckdb {

BufferEvictionNode::BufferEvictionNode(weak_ptr<BlockHandle> handle_p, idx_t eviction_seq_num)
    : handle(std::move(handle_p)), handle_sequence_number(eviction_seq_num) {
	D_ASSERT(!handle.expired());
}

bool BufferEvictionNode::CanUnload(BlockHandle &handle_p) {
	if (handle_sequence_number != handle_p.eviction_seq_num) {
		// handle was used in between
		return false;
	}
	return handle_p.CanUnload();
}

shared_ptr<BlockHandle> BufferEvictionNode::TryGetBlockHandle() {
	auto handle_p = handle.lock();
	if (!handle_p) {
		// BlockHandle has been destroyed
		return nullptr;
	}
	if (!CanUnload(*handle_p)) {
		// handle was used in between
		return nullptr;
	}
	// this is the latest node in the queue with this handle
	return handle_p;
}

typedef duckdb_moodycamel::ConcurrentQueue<BufferEvictionNode> eviction_queue_t;

struct EvictionQueue {
public:
	EvictionQueue() : evict_queue_insertions(0), total_dead_nodes(0) {
	}

public:
	//! Add a buffer handle to the eviction queue. Returns true, if the queue is
	//! ready to be purged, and false otherwise.
	bool AddToEvictionQueue(BufferEvictionNode &&node);
	//! Tries to dequeue an element from the eviction queue, but only after acquiring the purge queue lock.
	bool TryDequeueWithLock(BufferEvictionNode &node);
	//! Garbage collect dead nodes in the eviction queue.
	void Purge();
	template <typename FN>
	void IterateUnloadableBlocks(FN fn);

	//! Increment the dead node counter in the purge queue.
	inline void IncrementDeadNodes() {
		total_dead_nodes++;
	}
	//! Decrement the dead node counter in the purge queue.
	inline void DecrementDeadNodes() {
		total_dead_nodes--;
	}

private:
	//! Bulk purge dead nodes from the eviction queue. Then, enqueue those that are still alive.
	void PurgeIteration(const idx_t purge_size);

public:
	//! The concurrent queue
	eviction_queue_t q;

private:
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

private:
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

bool EvictionQueue::AddToEvictionQueue(BufferEvictionNode &&node) {
	q.enqueue(std::move(node));
	return ++evict_queue_insertions % INSERT_INTERVAL == 0;
}

bool EvictionQueue::TryDequeueWithLock(BufferEvictionNode &node) {
	lock_guard<mutex> lock(purge_lock);
	return q.try_dequeue(node);
}

void EvictionQueue::Purge() {
	// only one thread purges the queue, all other threads early-out
	if (!purge_lock.try_lock()) {
		return;
	}
	lock_guard<mutex> lock {purge_lock, std::adopt_lock};

	// we purge INSERT_INTERVAL * PURGE_SIZE_MULTIPLIER nodes
	idx_t purge_size = INSERT_INTERVAL * PURGE_SIZE_MULTIPLIER;

	// get an estimate of the queue size as-of now
	idx_t approx_q_size = q.size_approx();

	// early-out, if the queue is not big enough to justify purging
	// - we want to keep the LRU characteristic alive
	if (approx_q_size < purge_size * EARLY_OUT_MULTIPLIER) {
		return;
	}

	// There are two types of situations.

	// For most scenarios, purging INSERT_INTERVAL * PURGE_SIZE_MULTIPLIER nodes is enough.
	// Purging more nodes than we insert also counters oscillation for scenarios where most nodes are dead.
	// If we always purge slightly more, we trigger a purge less often, as we purge below the trigger.

	// However, if the pressure on the queue becomes too contested, we need to purge more aggressively,
	// i.e., we actively seek a specific number of dead nodes to purge. We use the total number of existing dead nodes.
	// We detect this situation by observing the queue's ratio between alive vs. dead nodes. If the ratio of alive vs.
	// dead nodes grows faster than we can purge, we keep purging until we hit one of the following conditions.

	// 2.1. We're back at an approximate queue size less than purge_size * EARLY_OUT_MULTIPLIER.
	// 2.2. We're back at a ratio of 1*alive_node:ALIVE_NODE_MULTIPLIER*dead_nodes.
	// 2.3. We've purged the entire queue: max_purges is zero. This is a worst-case scenario,
	// guaranteeing that we always exit the loop.

	idx_t max_purges = approx_q_size / purge_size;
	while (max_purges != 0) {
		PurgeIteration(purge_size);

		// update relevant sizes and potentially early-out
		approx_q_size = q.size_approx();

		// early-out according to (2.1)
		if (approx_q_size < purge_size * EARLY_OUT_MULTIPLIER) {
			break;
		}

		idx_t approx_dead_nodes = total_dead_nodes;
		approx_dead_nodes = approx_dead_nodes > approx_q_size ? approx_q_size : approx_dead_nodes;
		idx_t approx_alive_nodes = approx_q_size - approx_dead_nodes;

		// early-out according to (2.2)
		if (approx_alive_nodes * (ALIVE_NODE_MULTIPLIER - 1) > approx_dead_nodes) {
			break;
		}

		max_purges--;
	}
}

void EvictionQueue::PurgeIteration(const idx_t purge_size) {
	// if this purge is significantly smaller or bigger than the previous purge, then
	// we need to resize the purge_nodes vector. Note that this barely happens, as we
	// purge queue_insertions * PURGE_SIZE_MULTIPLIER nodes
	idx_t previous_purge_size = purge_nodes.size();
	if (purge_size < previous_purge_size / 2 || purge_size > previous_purge_size) {
		purge_nodes.resize(purge_size);
	}

	// bulk purge
	idx_t actually_dequeued = q.try_dequeue_bulk(purge_nodes.begin(), purge_size);

	// retrieve all alive nodes that have been wrongly dequeued
	idx_t alive_nodes = 0;
	for (idx_t i = 0; i < actually_dequeued; i++) {
		auto &node = purge_nodes[i];
		auto handle = node.TryGetBlockHandle();
		if (handle) {
			q.enqueue(std::move(node));
			alive_nodes++;
		}
	}

	total_dead_nodes -= actually_dequeued - alive_nodes;
}

BufferPool::BufferPool(idx_t maximum_memory, bool track_eviction_timestamps,
                       idx_t allocator_bulk_deallocation_flush_threshold)
    : maximum_memory(maximum_memory),
      allocator_bulk_deallocation_flush_threshold(allocator_bulk_deallocation_flush_threshold),
      track_eviction_timestamps(track_eviction_timestamps),
      temporary_memory_manager(make_uniq<TemporaryMemoryManager>()) {
	queues.reserve(FILE_BUFFER_TYPE_COUNT);
	for (idx_t i = 0; i < FILE_BUFFER_TYPE_COUNT; i++) {
		queues.push_back(make_uniq<EvictionQueue>());
	}
}
BufferPool::~BufferPool() {
}

bool BufferPool::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	auto &queue = GetEvictionQueueForType(handle->buffer->type);

	// The block handle is locked during this operation (Unpin),
	// or the block handle is still a local variable (ConvertToPersistent)
	D_ASSERT(handle->readers == 0);
	auto ts = ++handle->eviction_seq_num;
	if (track_eviction_timestamps) {
		handle->lru_timestamp_msec =
		    std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		        .time_since_epoch()
		        .count();
	}

	if (ts != 1) {
		// we add a newer version, i.e., we kill exactly one previous version
		queue.IncrementDeadNodes();
	}

	// Get the eviction queue for the buffer type and add it
	return queue.AddToEvictionQueue(BufferEvictionNode(weak_ptr<BlockHandle>(handle), ts));
}

EvictionQueue &BufferPool::GetEvictionQueueForType(FileBufferType type) {
	return *queues[uint8_t(type) - 1];
}

void BufferPool::IncrementDeadNodes(FileBufferType type) {
	GetEvictionQueueForType(type).IncrementDeadNodes();
}

void BufferPool::UpdateUsedMemory(MemoryTag tag, int64_t size) {
	memory_usage.UpdateUsedMemory(tag, size);
}

idx_t BufferPool::GetUsedMemory() const {
	return memory_usage.GetUsedMemory(MemoryUsageCaches::FLUSH);
}

idx_t BufferPool::GetMaxMemory() const {
	return maximum_memory;
}

idx_t BufferPool::GetQueryMaxMemory() const {
	return GetMaxMemory();
}

TemporaryMemoryManager &BufferPool::GetTemporaryMemoryManager() {
	return *temporary_memory_manager;
}

BufferPool::EvictionResult BufferPool::EvictBlocks(MemoryTag tag, idx_t extra_memory, idx_t memory_limit,
                                                   unique_ptr<FileBuffer> *buffer) {
	// First, we try to evict persistent table data
	auto block_result =
	    EvictBlocksInternal(GetEvictionQueueForType(FileBufferType::BLOCK), tag, extra_memory, memory_limit, buffer);
	if (block_result.success) {
		return block_result;
	}

	// If that does not succeed, we try to evict temporary data
	auto managed_buffer_result = EvictBlocksInternal(GetEvictionQueueForType(FileBufferType::MANAGED_BUFFER), tag,
	                                                 extra_memory, memory_limit, buffer);
	if (managed_buffer_result.success) {
		return managed_buffer_result;
	}

	// Finally, we try to evict tiny buffers
	return EvictBlocksInternal(GetEvictionQueueForType(FileBufferType::TINY_BUFFER), tag, extra_memory, memory_limit,
	                           buffer);
}

BufferPool::EvictionResult BufferPool::EvictBlocksInternal(EvictionQueue &queue, MemoryTag tag, idx_t extra_memory,
                                                           idx_t memory_limit, unique_ptr<FileBuffer> *buffer) {
	TempBufferPoolReservation r(tag, *this, extra_memory);
	bool found = false;

	if (memory_usage.GetUsedMemory(MemoryUsageCaches::NO_FLUSH) <= memory_limit) {
		if (Allocator::SupportsFlush() && extra_memory > allocator_bulk_deallocation_flush_threshold) {
			Allocator::FlushAll();
		}
		return {true, std::move(r)};
	}

	queue.IterateUnloadableBlocks([&](BufferEvictionNode &, const shared_ptr<BlockHandle> &handle) {
		// hooray, we can unload the block
		if (buffer && handle->buffer->AllocSize() == extra_memory) {
			// we can re-use the memory directly
			*buffer = handle->UnloadAndTakeBlock();
			found = true;
			return false;
		}

		// release the memory and mark the block as unloaded
		handle->Unload();

		if (memory_usage.GetUsedMemory(MemoryUsageCaches::NO_FLUSH) <= memory_limit) {
			found = true;
			return false;
		}

		// Continue iteration
		return true;
	});

	if (!found) {
		r.Resize(0);
	} else if (Allocator::SupportsFlush() && extra_memory > allocator_bulk_deallocation_flush_threshold) {
		Allocator::FlushAll();
	}

	return {found, std::move(r)};
}

idx_t BufferPool::PurgeAgedBlocks(uint32_t max_age_sec) {
	int64_t now = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                  .time_since_epoch()
	                  .count();
	int64_t limit = now - (static_cast<int64_t>(max_age_sec) * 1000);
	idx_t purged_bytes = 0;
	for (auto &queue : queues) {
		purged_bytes += PurgeAgedBlocksInternal(*queue, max_age_sec, now, limit);
	}
	return purged_bytes;
}

idx_t BufferPool::PurgeAgedBlocksInternal(EvictionQueue &queue, uint32_t max_age_sec, int64_t now, int64_t limit) {
	idx_t purged_bytes = 0;
	queue.IterateUnloadableBlocks([&](BufferEvictionNode &node, const shared_ptr<BlockHandle> &handle) {
		// We will unload this block regardless. But stop the iteration immediately afterward if this
		// block is younger than the age threshold.
		bool is_fresh = handle->lru_timestamp_msec >= limit && handle->lru_timestamp_msec <= now;
		purged_bytes += handle->GetMemoryUsage();
		handle->Unload();
		return is_fresh;
	});
	return purged_bytes;
}

template <typename FN>
void EvictionQueue::IterateUnloadableBlocks(FN fn) {
	for (;;) {
		// get a block to unpin from the queue
		BufferEvictionNode node;
		if (!q.try_dequeue(node)) {
			// we could not dequeue any eviction node, so we try one more time,
			// but more aggressively
			if (!TryDequeueWithLock(node)) {
				return;
			}
		}

		// get a reference to the underlying block pointer
		auto handle = node.TryGetBlockHandle();
		if (!handle) {
			DecrementDeadNodes();
			continue;
		}

		// we might be able to free this block: grab the mutex and check if we can free it
		lock_guard<mutex> lock(handle->lock);
		if (!node.CanUnload(*handle)) {
			// something changed in the mean-time, bail out
			DecrementDeadNodes();
			continue;
		}

		if (!fn(node, handle)) {
			break;
		}
	}
}

void BufferPool::PurgeQueue(FileBufferType type) {
	GetEvictionQueueForType(type).Purge();
}

void BufferPool::SetLimit(idx_t limit, const char *exception_postscript) {
	lock_guard<mutex> l_lock(limit_lock);
	// try to evict until the limit is reached
	if (!EvictBlocks(MemoryTag::EXTENSION, 0, limit).success) {
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    exception_postscript);
	}
	idx_t old_limit = maximum_memory;
	// set the global maximum memory to the new limit if successful
	maximum_memory = limit;
	// evict again
	if (!EvictBlocks(MemoryTag::EXTENSION, 0, limit).success) {
		// failed: go back to old limit
		maximum_memory = old_limit;
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    exception_postscript);
	}
	if (Allocator::SupportsFlush()) {
		Allocator::FlushAll();
	}
}

void BufferPool::SetAllocatorBulkDeallocationFlushThreshold(idx_t threshold) {
	allocator_bulk_deallocation_flush_threshold = threshold;
}

idx_t BufferPool::GetAllocatorBulkDeallocationFlushThreshold() {
	return allocator_bulk_deallocation_flush_threshold;
}

BufferPool::MemoryUsage::MemoryUsage() {
	for (auto &v : memory_usage) {
		v = 0;
	}
	for (auto &cache : memory_usage_caches) {
		for (auto &v : cache) {
			v = 0;
		}
	}
}

void BufferPool::MemoryUsage::UpdateUsedMemory(MemoryTag tag, int64_t size) {
	auto tag_idx = (idx_t)tag;
	if ((idx_t)AbsValue(size) < MEMORY_USAGE_CACHE_THRESHOLD) {
		// update cache and update global counter when cache exceeds threshold
		// Get corresponding cache slot based on current CPU core index
		// Two threads may access the same cache simultaneously,
		// ensuring correctness through atomic operations
		auto cache_idx = (idx_t)TaskScheduler::GetEstimatedCPUId() % MEMORY_USAGE_CACHE_COUNT;
		auto &cache = memory_usage_caches[cache_idx];
		auto new_tag_size = cache[tag_idx].fetch_add(size, std::memory_order_relaxed) + size;
		if ((idx_t)AbsValue(new_tag_size) >= MEMORY_USAGE_CACHE_THRESHOLD) {
			// cached tag memory usage exceeds threshold
			auto tag_size = cache[tag_idx].exchange(0, std::memory_order_relaxed);
			memory_usage[tag_idx].fetch_add(tag_size, std::memory_order_relaxed);
		}
		auto new_total_size = cache[TOTAL_MEMORY_USAGE_INDEX].fetch_add(size, std::memory_order_relaxed) + size;
		if ((idx_t)AbsValue(new_total_size) >= MEMORY_USAGE_CACHE_THRESHOLD) {
			// cached total memory usage exceeds threshold
			auto total_size = cache[TOTAL_MEMORY_USAGE_INDEX].exchange(0, std::memory_order_relaxed);
			memory_usage[TOTAL_MEMORY_USAGE_INDEX].fetch_add(total_size, std::memory_order_relaxed);
		}
	} else {
		// update global counter
		memory_usage[tag_idx].fetch_add(size, std::memory_order_relaxed);
		memory_usage[TOTAL_MEMORY_USAGE_INDEX].fetch_add(size, std::memory_order_relaxed);
	}
}

} // namespace duckdb
