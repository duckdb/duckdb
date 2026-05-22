//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache/external_file_cache_block_memory.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/buffer/block_handle.hpp"

#include <functional>

namespace duckdb {

class ExternalFileCacheBlockMemory : public BlockMemory {
public:
	ExternalFileCacheBlockMemory(BufferManager &buffer_manager, block_id_t block_id, MemoryTag tag,
	                             unique_ptr<FileBuffer> buffer, DestroyBufferUpon destroy_buffer_upon, idx_t size,
	                             BufferPoolReservation &&reservation, std::function<void()> on_load,
	                             std::function<void()> on_unload);
	~ExternalFileCacheBlockMemory() override;

protected:
	void OnUnload() override;

private:
	std::function<void()> on_load;
	std::function<void()> on_unload;
};

} // namespace duckdb
