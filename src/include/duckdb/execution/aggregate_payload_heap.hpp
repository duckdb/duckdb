//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/aggregate_payload_heap.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

//! Handle to a string payload stored on a registered payload block
struct AggregatePayloadHandle {
	uint32_t block_idx;
	uint32_t offset;
	uint32_t length;
};

//! Shared registry of buffer-managed blocks holding string payloads owned by aggregate states.
//! Blocks are unpinned once full, so the buffer manager can offload them to temporary storage
//! instead of failing the query when the payloads exceed the memory limit.
class AggregatePayloadRegistry {
public:
	explicit AggregatePayloadRegistry(BufferManager &buffer_manager);

	//! Register a block, returns its index
	uint32_t RegisterBlock(shared_ptr<BlockHandle> block);
	//! Pin a registered block for reading
	BufferHandle PinBlock(uint32_t block_idx);

public:
	BufferManager &buffer_manager;

private:
	mutex lock;
	vector<shared_ptr<BlockHandle>> blocks;
};

//! Per-thread writer over the shared registry. Only the block currently being written stays pinned.
class AggregatePayloadHeap {
public:
	explicit AggregatePayloadHeap(AggregatePayloadRegistry &registry);

	//! Append a payload, the returned handle stays valid for the registry's lifetime
	AggregatePayloadHandle Append(const char *data, uint32_t length);
	//! Pin the block of a handle and return a pointer to the payload (valid while the pin is held)
	const char *Read(const AggregatePayloadHandle &handle, BufferHandle &pin);

public:
	AggregatePayloadRegistry &registry;

private:
	BufferHandle current_pin;
	uint32_t current_block_idx = 0;
	idx_t current_offset = 0;
	idx_t current_capacity = 0;
};

} // namespace duckdb
