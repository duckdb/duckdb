#include "duckdb/storage/buffer/buffer_pool.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

namespace duckdb {

typedef duckdb_moodycamel::ConcurrentQueue<BufferEvictionNode> eviction_queue_t;

struct EvictionQueue {
	eviction_queue_t q;
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
    : current_memory(0), maximum_memory(maximum_memory), queue(make_uniq<EvictionQueue>()),
      temporary_memory_manager(make_uniq<TemporaryMemoryManager>()), evict_queue_insertions(0), purge_active(false) {
	for (idx_t i = 0; i < MEMORY_TAG_COUNT; i++) {
		memory_usage_per_tag[i] = 0;
	}
}
BufferPool::~BufferPool() {
}

void BufferPool::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	constexpr int INSERT_INTERVAL = 1024;

	D_ASSERT(handle->readers == 0);
	handle->eviction_timestamp++;

	// After INSERT_INTERVAL insertions, try running through the queue and purge.
	if (++evict_queue_insertions % INSERT_INTERVAL == 0) {
		PurgeQueue();
	}

	queue->q.enqueue(BufferEvictionNode(weak_ptr<BlockHandle>(handle), handle->eviction_timestamp));
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
	BufferEvictionNode node;
	TempBufferPoolReservation r(tag, *this, extra_memory);
	while (current_memory > memory_limit) {
		// get a block to unpin from the queue
		if (!queue->q.try_dequeue(node)) {
			// Failed to reserve. Adjust size of temp reservation to 0.
			r.Resize(0);
			return {false, std::move(r)};
		}
		// get a reference to the underlying block pointer
		auto handle = node.TryGetBlockHandle();
		if (!handle) {
			continue;
		}
		// we might be able to free this block: grab the mutex and check if we can free it
		lock_guard<mutex> lock(handle->lock);
		if (!node.CanUnload(*handle)) {
			// something changed in the mean-time, bail out
			continue;
		}
		// hooray, we can unload the block
		if (buffer && handle->buffer->AllocSize() == extra_memory) {
			// we can actually re-use the memory directly!
			*buffer = handle->UnloadAndTakeBlock();
			return {true, std::move(r)};
		} else {
			// release the memory and mark the block as unloaded
			handle->Unload();
		}
	}
	return {true, std::move(r)};
}

void BufferPool::PurgeQueue() {

	// we trigger a purge every INSERT_INTERVAL insertions into the queue
	// we assume that there are alive nodes in the queue, so we never want to purge the whole queue

	// only one thread purges the queue, all other threads early-out
	bool actual_purge_active;
	do {
		actual_purge_active = purge_active;
		if (actual_purge_active) {
			return;
		}
	} while (!std::atomic_compare_exchange_weak(&purge_active, &actual_purge_active, true));

	auto approx_q_size = queue->q.size_approx();

	// defensive check, nothing to purge
	if (approx_q_size < BULK_PURGE_SIZE) {
		purge_active = false;
		return;
	}

	// the purge queue is a concurrent structure, so we need to attempt purging the whole queue,
	// as of the approx_q_size seen. We early-out, if we see too many alive nodes. If not, we
	// purge as much as we reasonably can, as we assume that new dead nodes are created while purging.
	idx_t max_purges = approx_q_size / BULK_PURGE_SIZE;
	idx_t alive_nodes = 0;
	while (max_purges && alive_nodes < BULK_PURGE_THRESHOLD) {

		// bulk dequeue
		auto actually_dequeued = queue->q.try_dequeue_bulk(purge_nodes.begin(), BULK_PURGE_SIZE);

		// retrieve all alive nodes that have been wrongly dequeued
		alive_nodes = 0;
		for (idx_t i = 0; i < actually_dequeued; i++) {
			auto &node = purge_nodes[i];
			auto handle = node.TryGetBlockHandle();
			if (handle) {
				purge_nodes[alive_nodes++] = std::move(node);
			}
		}

		// bulk enqueue
		queue->q.enqueue_bulk(purge_nodes.begin(), alive_nodes);
		max_purges--;
	}

	// allows other threads to purge again
	purge_active = false;
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
