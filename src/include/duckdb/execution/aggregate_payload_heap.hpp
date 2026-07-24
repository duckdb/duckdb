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
	//! Read a payload (valid until the next Read or Append on this heap).
	//! Subsequent reads mostly hit the same block, so the pin of the last read block is cached.
	const char *Read(const AggregatePayloadHandle &handle);

public:
	AggregatePayloadRegistry &registry;

private:
	BufferHandle current_pin;
	uint32_t current_block_idx = 0;
	idx_t current_offset = 0;
	idx_t current_capacity = 0;
	//! Cached pin of the block last read from
	BufferHandle read_pin;
	uint32_t read_block_idx = 0;
	bool read_pin_valid = false;
};

} // namespace duckdb
