#include "duckdb/storage/cbuffer_manager.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"

namespace duckdb {

CBufferManager::CBufferManager(CBufferManagerConfig config) : VirtualBufferManager(), config(move(config)) {
	block_manager = make_unique<InMemoryBlockManager>(*this);
}

BufferHandle CBufferManager::Allocate(idx_t block_size, bool can_destroy = true,
                                      shared_ptr<BlockHandle> *block = nullptr) {
	if (!can_destroy) {
		throw InvalidInputException(
		    "When using a callback-based BufferManager, we don't support creating temporary files");
	}
	auto allocation = config.allocate_func(config.data, block_size);
	shared_ptr<BlockHandle> temp_block; // Doesn't this cause a memory-leak, or at the very least heap-use-after-free???
	shared_ptr<BlockHandle> *handle_p = block ? block : &temp_block;

	auto file_buffer = make_shared<FileBuffer>();

	*handle_p = make_shared<BlockHandle>(*block_manager, ++temporary_id);
	auto &handle_ref = **handle_p;
	handle_ref.
}

//! FIXME: Maybe make this non-pure and just call Destroy and Allocate?
void CBufferManager::ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) {
}

//! FIXME: Missing prototype for Destroy??
BufferHandle CBufferManager::Pin(shared_ptr<BlockHandle> &handle) {
}

void CBufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
}

idx_t CBufferManager::GetUsedMemory() const {
}

idx_t CBufferManager::GetMaxMemory() const {
}

} // namespace duckdb
