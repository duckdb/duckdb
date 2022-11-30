#include "duckdb/storage/cbuffer_manager.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/common/external_file_buffer.hpp"

namespace duckdb {

CBufferManager::CBufferManager(CBufferManagerConfig config_p)
    : VirtualBufferManager(), config(move(config_p)),
      custom_allocator(CBufferAllocatorAllocate, CBufferAllocatorFree, CBufferAllocatorRealloc,
                       make_unique<CBufferAllocatorData>(*this)) {
	block_manager = make_unique<InMemoryBlockManager>(*this);
}

BufferHandle CBufferManager::Allocate(idx_t block_size, bool can_destroy = true,
                                      shared_ptr<BlockHandle> *block = nullptr) {
	if (!can_destroy) {
		throw InvalidInputException(
		    "When using a callback-based BufferManager, we don't support creating temporary files");
	}
	idx_t alloc_size = BufferManager::GetAllocSize(block_size);
	auto allocation = config.allocate_func(config.data, block_size);
	shared_ptr<BlockHandle> temp_block; // Doesn't this cause a memory-leak, or at the very least heap-use-after-free???
	shared_ptr<BlockHandle> *handle_p = block ? block : &temp_block;

	auto buffer = make_shared<ExternalFileBuffer>(custom_allocator, config, alloc_size);
	BufferPoolReservation reservation;
	reservation.size = alloc_size;

	// create a new block pointer for this block
	*handle_p = make_shared<BlockHandle>(*block_manager, ++temporary_id, move(buffer), can_destroy, alloc_size,
	                                     move(reservation));
	return Pin(*handle_p);
}

//! FIXME: Maybe make this non-pure and just call Destroy and Allocate?
void CBufferManager::ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) {
}

//! FIXME: Missing prototype for Destroy??
BufferHandle CBufferManager::Pin(shared_ptr<BlockHandle> &handle) {
	auto &buffer = (ExternalFileBuffer &)*handle->buffer;
	config.pin_func(buffer.ExternalBufferHandle());
	return handle->Load(handle);
}

void CBufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
	auto &buffer = (ExternalFileBuffer &)*handle->buffer;
	config.unpin_func(buffer.ExternalBufferHandle());
}

idx_t CBufferManager::GetUsedMemory() const {
	return config.used_memory_func();
}

idx_t CBufferManager::GetMaxMemory() const {
	return config.max_memory_func();
}

//===--------------------------------------------------------------------===//
// Buffer Allocator
//===--------------------------------------------------------------------===//
data_ptr_t CBufferManager::CBufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
	auto &data = (CBufferAllocatorData &)*private_data;
	auto &config = data.manager.config;
	duckdb_buffer buffer = config.allocate_func(config.data, size);
	return (data_ptr_t)buffer;
}

void CBufferManager::CBufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	auto &data = (CBufferAllocatorData &)*private_data;
	auto &config = data.manager.config;
	config.destroy_func(pointer);
}

data_ptr_t CBufferManager::CBufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer,
                                                   idx_t old_size, idx_t size) {
	auto &data = (CBufferAllocatorData &)*private_data;
	auto &config = data.manager.config;
	auto buffer = config.reallocate_func(pointer, old_size, size);
	return (data_ptr_t)buffer;
}

} // namespace duckdb
