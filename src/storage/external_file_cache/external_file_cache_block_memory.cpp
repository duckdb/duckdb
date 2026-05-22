#include "duckdb/storage/external_file_cache/external_file_cache_block_memory.hpp"

namespace duckdb {

ExternalFileCacheBlockMemory::ExternalFileCacheBlockMemory(BufferManager &buffer_manager, block_id_t block_id,
                                                           MemoryTag tag, unique_ptr<FileBuffer> buffer,
                                                           DestroyBufferUpon destroy_buffer_upon, idx_t size,
                                                           BufferPoolReservation &&reservation,
                                                           std::function<void()> on_load_p,
                                                           std::function<void()> on_unload_p)
    : BlockMemory(buffer_manager, block_id, tag, std::move(buffer), destroy_buffer_upon, size, std::move(reservation)),
      on_load(std::move(on_load_p)), on_unload(std::move(on_unload_p)) {
	OnLoad();
}

ExternalFileCacheBlockMemory::~ExternalFileCacheBlockMemory() {
	if (GetBuffer() && GetState() == BlockState::BLOCK_LOADED) {
		OnUnload();
	}
}

void ExternalFileCacheBlockMemory::OnLoad() {
	if (on_load) {
		on_load();
	}
}

void ExternalFileCacheBlockMemory::OnUnload() {
	if (on_unload) {
		on_unload();
	}
}

} // namespace duckdb
