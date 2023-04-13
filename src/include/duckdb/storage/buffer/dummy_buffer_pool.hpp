#pragma once

#include "duckdb/storage/buffer/buffer_pool.hpp"

namespace duckdb {

//! A BufferPool implementation that does nothing
class DummyBufferPool : public BufferPool {
public:
	explicit DummyBufferPool() {
	}
	virtual ~DummyBufferPool() {
	}

	void SetLimit(idx_t limit, const char *exception_postscript) final override {};
	idx_t GetUsedMemory() final override {
		throw InvalidInputException("This bufferpool does not keep track of used memory");
	}
	idx_t GetMaxMemory() final override {
		throw InvalidInputException("This bufferpool does not keep track of maximum memory");
	}
	void IncreaseUsedMemory(idx_t size) final override {};
	void DecreaseUsedMemory(idx_t size) final override {};

	BufferPool::EvictionResult EvictBlocks(idx_t extra_memory, idx_t memory_limit,
	                                       unique_ptr<FileBuffer> *buffer = nullptr) final override {
		throw InvalidInputException("This bufferpool can't evict blocks");
	}

	//! Garbage collect eviction queue
	void PurgeQueue() final override {};
	void AddToEvictionQueue(shared_ptr<BlockHandle> &handle) final override {};
};

} // namespace duckdb
