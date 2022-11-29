#include "duckdb/storage/cbuffer_manager.hpp"

namespace duckdb {

CBufferManager::CBufferManager(CBufferManagerConfig config) : VirtualBufferManager(), config(move(config)) {
}

BufferHandle CBufferManager::Allocate(idx_t block_size, bool can_destroy = true,
                                      shared_ptr<BlockHandle> *block = nullptr) {
	if (!can_destroy) {
		throw InvalidInputException(
		    "When using a callback-based BufferManager, we don't support creating temporary files");
	}
	auto allocation = config.allocate_func()
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
