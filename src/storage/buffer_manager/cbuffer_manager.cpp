#include "duckdb/storage/cbuffer_manager.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/common/external_file_buffer.hpp"

namespace duckdb {

Allocator &CBufferManager::GetBufferAllocator() {
	return allocator;
}

CBufferManager::CBufferManager(CBufferManagerConfig config_p)
    : VirtualBufferManager(), config(move(config_p)),
      custom_allocator(CBufferAllocatorAllocate, CBufferAllocatorFree, CBufferAllocatorRealloc,
                       make_unique<CBufferAllocatorData>(*this)) {
	block_manager = make_unique<InMemoryBlockManager>(*this);
}

BufferHandle CBufferManager::Allocate(idx_t block_size, bool can_destroy, shared_ptr<BlockHandle> *block) {
	// if (!can_destroy) {
	//	throw InvalidInputException(
	//	    "When using a callback-based BufferManager, we don't support creating temporary files");
	// }
	idx_t alloc_size = BufferManager::GetAllocSize(block_size);
	shared_ptr<BlockHandle> temp_block; // Doesn't this cause a memory-leak, or at the very least heap-use-after-free???
	shared_ptr<BlockHandle> *handle_p = block ? block : &temp_block;

	// Create an ExternalFileBuffer, which uses a callback to retrieve the allocation when Buffer() is called
	auto buffer = make_unique<ExternalFileBuffer>(custom_allocator, config, alloc_size);
	BufferPoolReservation reservation;
	reservation.size = alloc_size;

	// create a new block pointer for this block
	*handle_p = make_shared<BlockHandle>(*block_manager, ++temporary_id, move(buffer), can_destroy, alloc_size,
	                                     move(reservation));
	return Pin(*handle_p);
}

shared_ptr<BlockHandle> CBufferManager::RegisterSmallMemory(idx_t block_size) {
	auto buffer = make_unique<ExternalFileBuffer>(custom_allocator, config, block_size);

	// create a new block pointer for this block
	BufferPoolReservation reservation;
	reservation.size = block_size;
	return make_shared<BlockHandle>(*block_manager, ++temporary_id, move(buffer), false, block_size, move(reservation));
}

//! FIXME: Maybe make this non-pure and just call Destroy and Allocate?
void CBufferManager::ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->state == BlockState::BLOCK_LOADED);
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
	D_ASSERT(handle->memory_usage == handle->memory_charge.size);

	auto req = handle->buffer->CalculateMemory(block_size);
	int64_t memory_delta = (int64_t)req.alloc_size - handle->memory_usage;

	// FIXME: we don't have an atomix<idx_t>, can't use Resize()
	// Resize also increases `used_memory`, which we can't do, we just have to trust that `reallocate_func` updates the
	// used memory
	handle->memory_charge.size = req.alloc_size;

	// resize and adjust current memory
	handle->buffer->Resize(block_size);
	handle->memory_usage += memory_delta;
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
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
	return config.used_memory_func(config.data);
}

idx_t CBufferManager::GetMaxMemory() const {
	return config.max_memory_func(config.data);
}

atomic<idx_t> &CBufferManager::GetMutableUsedMemory() {
	// FIXME: this is horrible

	// We don't want this to throw an exception, it should just be a no-op
	// So we have to have an `atomic<idx_t>` to return from this function, even if it's not used anywhere else
	return get_mutable_used_memory_enabler;
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
