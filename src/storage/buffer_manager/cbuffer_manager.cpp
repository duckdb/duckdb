#include "duckdb/storage/cbuffer_manager.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/common/external_file_buffer.hpp"

namespace duckdb {

Allocator &CBufferManager::GetBufferAllocator() {
	return allocator;
}

CBufferManager::CBufferManager(CBufferManagerConfig config_p)
    : VirtualBufferManager(), config(config_p),
      custom_allocator(CBufferAllocatorAllocate, CBufferAllocatorFree, CBufferAllocatorRealloc,
                       make_unique<CBufferAllocatorData>(*this)) {
	block_manager = make_unique<InMemoryBlockManager>(*this);
}

BufferHandle CBufferManager::Allocate(idx_t block_size, bool can_destroy, shared_ptr<BlockHandle> *block) {
	idx_t alloc_size = VirtualBufferManager::GetAllocSize(block_size);
	shared_ptr<BlockHandle> temp_block; // Doesn't this cause a memory-leak, or at the very least heap-use-after-free???
	shared_ptr<BlockHandle> *handle_p = block ? block : &temp_block;

	// Create an ExternalFileBuffer, which uses a callback to retrieve the allocation when Buffer() is called
	auto buffer = make_unique<ExternalFileBuffer>(custom_allocator, config, alloc_size);
	BufferPoolReservation reservation(*this);
	reservation.size = alloc_size;

	// create a new block pointer for this block
	*handle_p = make_shared<BlockHandle>(*block_manager, ++temporary_id, move(buffer), can_destroy, alloc_size,
	                                     move(reservation));
	return Pin(*handle_p);
}

shared_ptr<BlockHandle> CBufferManager::RegisterSmallMemory(idx_t block_size) {
	auto buffer = make_unique<ExternalFileBuffer>(custom_allocator, config, block_size);

	// create a new block pointer for this block
	BufferPoolReservation reservation(*this);
	reservation.size = block_size;
	return make_shared<BlockHandle>(*block_manager, ++temporary_id, move(buffer), false, block_size, move(reservation));
}

void CBufferManager::ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->state == BlockState::BLOCK_LOADED);
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
	D_ASSERT(handle->memory_usage == handle->memory_charge.size);

	auto req = handle->buffer->CalculateMemory(block_size);
	int64_t memory_delta = (int64_t)req.alloc_size - handle->memory_usage;

	handle->memory_charge.Resize(req.alloc_size);

	// resize and adjust current memory
	handle->buffer->Resize(block_size);
	handle->memory_usage += memory_delta;
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
}

BufferHandle CBufferManager::Pin(shared_ptr<BlockHandle> &handle) {
	auto &buffer = (ExternalFileBuffer &)*handle->buffer;
	config.pin_func(buffer.ExternalBufferHandle());
	handle->readers++;
	return handle->Load(handle);
}

void CBufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
	auto &buffer = (ExternalFileBuffer &)*handle->buffer;
	handle->readers--;
	config.unpin_func(buffer.ExternalBufferHandle());
}

idx_t CBufferManager::GetUsedMemory() const {
	return config.used_memory_func(config.data);
}

void CBufferManager::AdjustUsedMemory(int64_t amount) {
	// no op
}

idx_t CBufferManager::GetMaxMemory() const {
	return config.max_memory_func(config.data);
}

void CBufferManager::PurgeQueue() {
	// no op
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
