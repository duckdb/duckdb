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
      temporary_memory_manager(make_uniq<TemporaryMemoryManager>()), evict_queue_insertions(0), purge_active(false),
		pinned_buffers(0) {
	for (idx_t i = 0; i < MEMORY_TAG_COUNT; i++) {
		memory_usage_per_tag[i] = 0;
	}
}
BufferPool::~BufferPool() {
}

void BufferPool::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {

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

//idx_t BufferPool::PurgeIteration() {
//	// bulk dequeue
//	auto actually_dequeued = queue->q.try_dequeue_bulk(purge_nodes.begin(), BULK_PURGE_SIZE);
//
//	// retrieve all alive nodes that have been wrongly dequeued
//	idx_t alive_nodes = 0;
//	for (idx_t i = 0; i < actually_dequeued; i++) {
//		auto &node = purge_nodes[i];
//		auto handle = node.TryGetBlockHandle();
//		if (handle) {
//			purge_nodes[alive_nodes++] = std::move(node);
//		}
//	}
//
//	// bulk enqueue
//	queue->q.enqueue_bulk(purge_nodes.begin(), alive_nodes);
//	return alive_nodes;
//}
//
//void BufferPool::PurgeQueue() {
//
//	// we trigger a purge every INSERT_INTERVAL insertions into the queue
//	// we assume that there are alive nodes in the queue, so we never want to purge the whole queue
//
//	// only one thread purges the queue, all other threads early-out
//	bool actual_purge_active;
//	do {
//		actual_purge_active = purge_active;
//		if (actual_purge_active) {
//			return;
//		}
//	} while (!std::atomic_compare_exchange_weak(&purge_active, &actual_purge_active, true));
//
//	auto approx_q_size = queue->q.size_approx();
//
//	// defensive check, nothing to purge
//	if (approx_q_size < BULK_PURGE_SIZE) {
//		purge_active = false;
//		return;
//	}
//
//	// we need to brute purge the entire queue to avoid memory leaks
//	if (approx_q_size > max_queue_size) {
//
//		Printer::PrintF("approx size: %llu -- max size: %llu", approx_q_size, max_queue_size);
//
//		idx_t max_iterations = approx_q_size / BULK_PURGE_SIZE;
//		idx_t total_alive_nodes = 0;
//
//		for (idx_t i = 0; i < max_iterations; i++) {
//			auto alive_nodes = PurgeIteration();
//			total_alive_nodes += alive_nodes;
//		}
//
//		// increase the max_queue_size, if the workload requires a bigger queue
//		if (approx_q_size * ACTIVE_THRESHOLD < total_alive_nodes) {
//			Printer::PrintF("alive threshold: %llu -- total alive: %llu", idx_t(approx_q_size * ACTIVE_THRESHOLD),
//			                total_alive_nodes);
//			max_queue_size *= 2;
//			Printer::PrintF("NEW MAX QUEUE SIZE: %llu", max_queue_size);
//		}
//
//		// allows other threads to purge again
//		purge_active = false;
//		return;
//	}
//
//	// purge the queue until we encounter more than ACTIVE_THRESHOLD active nodes,
//	// if we never encounter enough alive nodes, then we purge half the queue
//	idx_t max_purges = approx_q_size / BULK_PURGE_SIZE / 2;
//	bool early_out = false;
//	while (max_purges && !early_out) {
//
//		auto alive_nodes = PurgeIteration();
//		max_purges--;
//
//		if (BULK_PURGE_SIZE * ACTIVE_THRESHOLD < alive_nodes) {
//			early_out = true;
//		}
//	}
//
//	// allows other threads to purge again
//	purge_active = false;
//}

void BufferPool::PurgeQueue() {

	// only one thread purges the queue, all other threads early-out
	bool actual_purge_active;
	do {
		actual_purge_active = purge_active;
		if (actual_purge_active) {
			return;
		}
	} while (!std::atomic_compare_exchange_weak(&purge_active, &actual_purge_active, true));

	// retrieve the active blocks
	auto pinned = pinned_buffers.load();
	auto unpinned = evict_queue_insertions.load();
	idx_t active_blocks = 0;
	if (pinned > unpinned) {
		active_blocks = pinned - unpinned;
	}

	// defensive check
	auto approx_q_size = queue->q.size_approx();
	if (approx_q_size < active_blocks) {
		// nothing to do
		purge_active = false;
		return;
	}

	auto approx_dead_nodes = approx_q_size - active_blocks;
	if (approx_dead_nodes < active_blocks * 2) {
		// nothing to do
		purge_active = false;
		return;
	}

	auto approx_dead_nodes_to_purge = approx_dead_nodes / 2;
	vector<BufferEvictionNode> nodes;
	nodes.resize(approx_dead_nodes_to_purge);

	// bulk purge
	auto actually_dequeued = queue->q.try_dequeue_bulk(nodes.begin(), approx_dead_nodes_to_purge);

	// retrieve all alive nodes that have been wrongly dequeued
	idx_t alive_nodes = 0;
	for (idx_t i = 0; i < actually_dequeued; i++) {
		auto &node = nodes[i];
		auto handle = node.TryGetBlockHandle();
		if (handle) {
			nodes[alive_nodes++] = std::move(node);
		}
	}

	// bulk enqueue
	queue->q.enqueue_bulk(nodes.begin(), alive_nodes);

	Printer::PrintF("approx q size: %llu", approx_q_size);
	Printer::PrintF("approx dead nodes: %llu", approx_dead_nodes);
	Printer::PrintF("actually_dequeued: %llu", actually_dequeued);
	Printer::PrintF("alive_nodes: %llu", alive_nodes);


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
