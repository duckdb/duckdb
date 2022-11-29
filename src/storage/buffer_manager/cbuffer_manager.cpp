#include "duckdb/storage/cbuffer_manager.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"

namespace duckdb {

CBufferManager::CBufferManager(CBufferManagerConfig config) :
	  VirtualBufferManager(),
	  config(move(config)),
	  custom_allocator() {
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

//===--------------------------------------------------------------------===//
// Buffer Allocator
//===--------------------------------------------------------------------===//
data_ptr_t CBufferManager::CBufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
	auto &data = (CBufferAllocatorData &)*private_data;
	duckdb_buffer buffer = data.manager.config.allocate_func(data.allocation_context, size);
	return data.config.get_buffer_allocation(buffer);
}

void CBufferManager::CBufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	auto &data = (CBufferAllocatorData &)*private_data;
	//TODO: Retrieve the buffer belonging to this allocation (from pointer -> duckdb_buffer)
	auto buffer = RetrieveBuffer(pointer);
	data.manager.config.destroy_func(buffer);
}

data_ptr_t CBufferManager::CBufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                                 idx_t size) {
	auto &data = (CBufferAllocatorData &)*private_data;
	//TODO: Retrieve the buffer belonging to this allocation (from pointer -> duckdb_buffer)
	auto buffer = RetrieveBuffer(pointer);
	buffer = data.manager.config.reallocate_func(buffer, old_size, size);
	return data.config.get_buffer_allocation(buffer);
}

} // namespace duckdb
