#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

//! The BufferPool is in charge of handling memory management for one or more databases. It defines memory limits
//! and implements priority eviction among all users of the pool.
class BufferPool {
public:
	struct EvictionResult {
		bool success;
		TempBufferPoolReservation reservation;
	};

public:
	explicit BufferPool() {
	}
	virtual ~BufferPool() {
	}

	virtual void SetLimit(idx_t limit, const char *exception_postscript) = 0;
	virtual idx_t GetUsedMemory() = 0;
	virtual idx_t GetMaxMemory() = 0;
	virtual void IncreaseUsedMemory(idx_t size) = 0;
	virtual void DecreaseUsedMemory(idx_t size) = 0;

	virtual EvictionResult EvictBlocks(idx_t extra_memory, idx_t memory_limit,
	                                   unique_ptr<FileBuffer> *buffer = nullptr) = 0;

	//! Garbage collect eviction queue
	virtual void PurgeQueue() = 0;
	virtual void AddToEvictionQueue(shared_ptr<BlockHandle> &handle) = 0;
};

} // namespace duckdb
