#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/common/exception.hpp"

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
    : current_memory(0), maximum_memory(maximum_memory), queue(make_uniq<EvictionQueue>()), queue_insertions(0) {
}
BufferPool::~BufferPool() {
}

void BufferPool::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	constexpr int INSERT_INTERVAL = 1024;

	D_ASSERT(handle->readers == 0);
	handle->eviction_timestamp++;
	// After each 1024 insertions, run through the queue and purge.
	if ((++queue_insertions % INSERT_INTERVAL) == 0) {
		PurgeQueue();
	}
	queue->q.enqueue(BufferEvictionNode(weak_ptr<BlockHandle>(handle), handle->eviction_timestamp));
}

void BufferPool::IncreaseUsedMemory(idx_t size) {
	current_memory += size;
}

idx_t BufferPool::GetUsedMemory() {
	return current_memory;
}
idx_t BufferPool::GetMaxMemory() {
	return maximum_memory;
}

BufferPool::EvictionResult BufferPool::EvictBlocks(idx_t extra_memory, idx_t memory_limit,
                                                   unique_ptr<FileBuffer> *buffer) {
	BufferEvictionNode node;
	TempBufferPoolReservation r(*this, extra_memory);
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
	BufferEvictionNode node;
	while (true) {
		if (!queue->q.try_dequeue(node)) {
			break;
		}
		auto handle = node.TryGetBlockHandle();
		if (!handle) {
			continue;
		} else {
			queue->q.enqueue(std::move(node));
			break;
		}
	}
}

void BufferPool::SetLimit(idx_t limit, const char *exception_postscript) {
	lock_guard<mutex> l_lock(limit_lock);
	// try to evict until the limit is reached
	if (!EvictBlocks(0, limit).success) {
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    exception_postscript);
	}
	idx_t old_limit = maximum_memory;
	// set the global maximum memory to the new limit if successful
	maximum_memory = limit;
	// evict again
	if (!EvictBlocks(0, limit).success) {
		// failed: go back to old limit
		maximum_memory = old_limit;
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    exception_postscript);
	}
}

} // namespace duckdb
