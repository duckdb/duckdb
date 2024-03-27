#include "duckdb/storage/buffer/buffer_pool.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

namespace duckdb {

typedef duckdb_moodycamel::ConcurrentQueue<BufferEvictionNode> eviction_queue_t;

struct EvictionQueue {
	EvictionQueue() : evict_queue_insertions(0), total_dead_nodes(0) {
	}
	eviction_queue_t q;

	//! Increment the dead node counter in the purge queue.
	inline void IncrementDeadNodes() {
		total_dead_nodes++;
	}
	//! Decrement the dead node counter in the purge queue.
	inline void DecrementDeadNodes() {
		total_dead_nodes--;
	}

	//! Total number of insertions into the eviction queue. This guides the schedule for calling PurgeQueue.
	atomic<idx_t> evict_queue_insertions;
	//! Total dead nodes in the eviction queue. There are two scenarios in which a node dies: (1) we destroy its block
	//! handle, or (2) we insert a newer version into the eviction queue.
	atomic<idx_t> total_dead_nodes;
};

bool BufferEvictionNode::CanUnload(BlockHandle &handle_p) {
	if (timestamp != handle_p.eviction_timestamp) {
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

BufferPool::BufferPool(idx_t maximum_memory)
    : current_memory(0), maximum_memory(maximum_memory), temporary_memory_manager(make_uniq<TemporaryMemoryManager>()) {
	for (idx_t i = 0; i < FILE_BUFFER_TYPE_COUNT; i++) {
		queues[i] = make_uniq<EvictionQueue>();
	}
	for (idx_t i = 0; i < MEMORY_TAG_COUNT; i++) {
		memory_usage_per_tag[i] = 0;
	}
}
BufferPool::~BufferPool() {
}

bool BufferPool::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {

	// The block handle is locked during this operation (Unpin),
	// or the block handle is still a local variable (ConvertToPersistent)

	D_ASSERT(handle->readers == 0);
	auto ts = ++handle->eviction_timestamp;

	auto &queue = GetEvictionQueueForType(handle->buffer->type);
	BufferEvictionNode evict_node(weak_ptr<BlockHandle>(handle), ts);
	queue.q.enqueue(evict_node);

	if (ts != 1) {
		// we add a newer version, i.e., we kill exactly one previous version
		queue.IncrementDeadNodes();
	}

	if (++queue.evict_queue_insertions % INSERT_INTERVAL == 0) {
		return true;
	}
	return false;
}

EvictionQueue &BufferPool::GetEvictionQueueForType(FileBufferType type) {
	return *queues[uint8_t(type)];
}

void BufferPool::IncrementDeadNodes(FileBufferType type) {
	GetEvictionQueueForType(type).IncrementDeadNodes();
}

void BufferPool::IncreaseUsedMemory(MemoryTag tag, idx_t size) {
	current_memory += size;
	memory_usage_per_tag[uint8_t(tag)] += size;
}

idx_t BufferPool::GetUsedMemory() const {
	return current_memory;
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
	auto block_result =
	    EvictBlocksInternal(GetEvictionQueueForType(FileBufferType::BLOCK), tag, extra_memory, memory_limit, buffer);
	if (block_result.success) {
		return block_result;
	}
	auto managed_buffer_result = EvictBlocksInternal(GetEvictionQueueForType(FileBufferType::MANAGED_BUFFER), tag,
	                                                 extra_memory, memory_limit, buffer);
	if (managed_buffer_result.success) {
		return managed_buffer_result;
	}

	return EvictBlocksInternal(GetEvictionQueueForType(FileBufferType::TINY_BUFFER), tag, extra_memory, memory_limit,
	                           buffer);
}

BufferPool::EvictionResult BufferPool::EvictBlocksInternal(EvictionQueue &queue, MemoryTag tag, idx_t extra_memory,
                                                           idx_t memory_limit, unique_ptr<FileBuffer> *buffer) {
	BufferEvictionNode node;
	TempBufferPoolReservation r(tag, *this, extra_memory);

	while (current_memory > memory_limit) {
		// get a block to unpin from the queue
		if (!queue.q.try_dequeue(node)) {
			// we could not dequeue any eviction node, so we try one more time,
			// but more aggressively
			if (!TryDequeueWithLock(queue, node)) {
				// still no success, we return
				r.Resize(0);
				return {false, std::move(r)};
			}
		}

		// get a reference to the underlying block pointer
		auto handle = node.TryGetBlockHandle();
		if (!handle) {
			queue.DecrementDeadNodes();
			continue;
		}

		// we might be able to free this block: grab the mutex and check if we can free it
		lock_guard<mutex> lock(handle->lock);
		if (!node.CanUnload(*handle)) {
			// something changed in the mean-time, bail out
			queue.DecrementDeadNodes();
			continue;
		}

		// hooray, we can unload the block
		if (buffer && handle->buffer->AllocSize() == extra_memory) {
			// we can re-use the memory directly
			*buffer = handle->UnloadAndTakeBlock();
			return {true, std::move(r)};
		}

		// release the memory and mark the block as unloaded
		handle->Unload();
	}
	return {true, std::move(r)};
}

bool BufferPool::TryDequeueWithLock(EvictionQueue &queue, BufferEvictionNode &node) {
	lock_guard<mutex> lock(purge_lock);
	return queue.q.try_dequeue(node);
}

void BufferPool::PurgeQueue() {
	for (idx_t i = 0; i < FILE_BUFFER_TYPE_COUNT; i++) {
		PurgeQueueInternal(*queues[i]);
	}
}

void BufferPool::PurgeQueueInternal(EvictionQueue &queue) {
	// only one thread purges the queue, all other threads early-out
	if (!purge_lock.try_lock()) {
		return;
	}
	lock_guard<mutex> lock {purge_lock, std::adopt_lock};

	// we purge INSERT_INTERVAL * PURGE_SIZE_MULTIPLIER nodes
	idx_t purge_size = INSERT_INTERVAL * PURGE_SIZE_MULTIPLIER;

	// get an estimate of the queue size as-of now
	idx_t approx_q_size = queue.q.size_approx();

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
		PurgeIteration(queue, purge_size);

		// update relevant sizes and potentially early-out
		approx_q_size = queue.q.size_approx();

		// early-out according to (2.1)
		if (approx_q_size < purge_size * EARLY_OUT_MULTIPLIER) {
			break;
		}

		idx_t approx_dead_nodes = queue.total_dead_nodes;
		approx_dead_nodes = approx_dead_nodes > approx_q_size ? approx_q_size : approx_dead_nodes;
		idx_t approx_alive_nodes = approx_q_size - approx_dead_nodes;

		// early-out according to (2.2)
		if (approx_alive_nodes * (ALIVE_NODE_MULTIPLIER - 1) > approx_dead_nodes) {
			break;
		}

		max_purges--;
	}
}

void BufferPool::PurgeIteration(EvictionQueue &queue, const idx_t purge_size) {
	// if this purge is significantly smaller or bigger than the previous purge, then
	// we need to resize the purge_nodes vector. Note that this barely happens, as we
	// purge queue_insertions * PURGE_SIZE_MULTIPLIER nodes
	idx_t previous_purge_size = purge_nodes.size();
	if (purge_size < previous_purge_size / 2 || purge_size > previous_purge_size) {
		purge_nodes.resize(purge_size);
	}

	// bulk purge
	idx_t actually_dequeued = queue.q.try_dequeue_bulk(purge_nodes.begin(), purge_size);

	// retrieve all alive nodes that have been wrongly dequeued
	idx_t alive_nodes = 0;
	for (idx_t i = 0; i < actually_dequeued; i++) {
		auto &node = purge_nodes[i];
		auto handle = node.TryGetBlockHandle();
		if (handle) {
			queue.q.enqueue(std::move(node));
			alive_nodes++;
		}
	}

	queue.total_dead_nodes -= actually_dequeued - alive_nodes;
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
}

} // namespace duckdb
