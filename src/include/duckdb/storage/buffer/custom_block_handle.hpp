#pragma once

#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/cbuffer_manager.hpp"

namespace duckdb {

//! BlockHandle that contains a custom block handle, to be used together with the CBufferManager
// when this block handle dies, it will destroy our handle to the externally managed buffer as well
class CustomBlockHandle : public BlockHandle {
public:
	// block_manager is entirely unused
	CustomBlockHandle(duckdb_block block, CBufferManagerConfig &config, BlockManager &block_manager,
	                  block_id_t block_id, unique_ptr<FileBuffer> buffer, bool can_destroy, idx_t block_size,
	                  BufferPoolReservation &&reservation)
	    : BlockHandle(block_manager, block_id, std::move(buffer), can_destroy, block_size, std::move(reservation),
	                  BlockHandleType::CUSTOM),
	      block(block), config(config) {
	}
	~CustomBlockHandle() {
		config.destroy_func(config.data, block);
	}

public:
	void ResizeBuffer(uint64_t block_size, int64_t memory_delta) {
		D_ASSERT(buffer);
		auto old_size = buffer->AllocSize();
		// Destroy the old buffer
		buffer.reset();
		// And create a new one, with the new size;
		config.reallocate_func(config.data, block, old_size, block_size);
		memory_usage += memory_delta;
		D_ASSERT(memory_usage == buffer->AllocSize());
	}

public:
	// The custom block handle tied to this block
	duckdb_block block;
	// The config with callbacks used to manage this block
	CBufferManagerConfig &config;
};

} // namespace duckdb
