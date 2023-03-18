#include "duckdb/storage/cbuffer_manager.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/common/external_file_buffer.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer/dummy_buffer_pool.hpp"

namespace duckdb {

Allocator &CBufferManager::GetBufferAllocator() {
	return allocator;
}

CBufferManager::CBufferManager(CBufferManagerConfig config_p)
    : BufferManager(), config(config_p),
      custom_allocator(CBufferAllocatorAllocate, CBufferAllocatorFree, CBufferAllocatorRealloc,
                       make_unique<CBufferAllocatorData>(*this)) {
	block_manager = make_unique<InMemoryBlockManager>(*this);
	buffer_pool = make_unique<DummyBufferPool>();
}

BufferHandle CBufferManager::Allocate(idx_t block_size, bool can_destroy, shared_ptr<BlockHandle> *block) {
	idx_t alloc_size = BufferManager::GetAllocSize(block_size);
	shared_ptr<BlockHandle> temp_block; // Doesn't this cause a memory-leak, or at the very least heap-use-after-free???
	shared_ptr<BlockHandle> *handle_p = block ? block : &temp_block;

	// create a new (UNLOADED) block pointer for this block
	*handle_p = make_shared<BlockHandle>(*block_manager, ++temporary_id);
	(*handle_p)->memory_usage = alloc_size;
	return Pin(*handle_p);
}

BufferPool &CBufferManager::GetBufferPool() {
	return *buffer_pool;
}

shared_ptr<BlockHandle> CBufferManager::RegisterSmallMemory(idx_t block_size) {
	auto buffer = make_unique<ExternalFileBuffer>(custom_allocator, config, block_size);

	// create a new block pointer for this block
	auto block = make_shared<BlockHandle>(*block_manager, ++temporary_id);
	block->memory_usage = block_size;
	return block;
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
	idx_t required_memory;
	{
		// lock the block
		lock_guard<mutex> lock(handle->lock);
		// check if the block is already loaded
		if (handle->state == BlockState::BLOCK_LOADED) {
			// the block is loaded, increment the reader count and return a pointer to the handle
			auto &buffer = (ExternalFileBuffer &)*handle->buffer;
			data_ptr_t allocation = (data_ptr_t)config.pin_func(config.data, buffer.ExternalBufferHandle());
			buffer.SetAllocation(allocation);
			handle->readers++;
			return handle->Load(handle);
		}
		required_memory = handle->memory_usage;
	}
	// Load the block, setting the allocation
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->readers == 0);
	handle->readers = 1;
	auto new_buffer = make_unique<ExternalFileBuffer>(custom_allocator, config, required_memory);
	TempBufferPoolReservation reservation(*buffer_pool, required_memory);
	handle->memory_charge = std::move(reservation);
	auto allocation = (data_ptr_t)(config.pin_func(config.data, new_buffer->ExternalBufferHandle()));

	// We have to do this manually, so 'Load' will just return the buffer
	new_buffer->SetAllocation(allocation);
	handle->buffer = std::move(new_buffer);
	handle->state = BlockState::BLOCK_LOADED;
	return handle->Load(handle);
}

void CBufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
	lock_guard<mutex> lock(handle->lock);
	if (handle->state == BlockState::BLOCK_UNLOADED) {
		return;
	}
	if (!handle->buffer) {
		return;
	}
	auto &buffer = (ExternalFileBuffer &)*handle->buffer;
	D_ASSERT(handle->readers > 0);
	handle->readers--;
	config.unpin_func(config.data, buffer.ExternalBufferHandle());
	if (handle->readers == 0 && handle->can_destroy) {
		handle.reset();
	}
}

idx_t CBufferManager::GetUsedMemory() const {
	return config.used_memory_func(config.data);
}

void CBufferManager::IncreaseUsedMemory(idx_t amount, bool unsafe) {
	// no op
}

void CBufferManager::DecreaseUsedMemory(idx_t amount) {
	// no op
}

idx_t CBufferManager::GetMaxMemory() const {
	return config.max_memory_func(config.data);
}

void CBufferManager::PurgeQueue() {
	// no op
}

void CBufferManager::DeleteTemporaryFile(block_id_t block_id) {
	// no op
}

//===--------------------------------------------------------------------===//
// Buffer Allocator
//===--------------------------------------------------------------------===//
data_ptr_t CBufferManager::CBufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
	auto &data = (CBufferAllocatorData &)*private_data;
	auto &config = data.manager.config;
	duckdb_buffer buffer = config.allocate_func(config.data, size, Storage::BLOCK_HEADER_SIZE);
	return (data_ptr_t)buffer;
}

void CBufferManager::CBufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	auto &data = (CBufferAllocatorData &)*private_data;
	auto &config = data.manager.config;
	config.destroy_func(config.data, pointer, Storage::BLOCK_HEADER_SIZE);
}

data_ptr_t CBufferManager::CBufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer,
                                                   idx_t old_size, idx_t size) {
	auto &data = (CBufferAllocatorData &)*private_data;
	auto &config = data.manager.config;
	auto buffer = config.reallocate_func(config.data, pointer, old_size, size, Storage::BLOCK_HEADER_SIZE);
	return (data_ptr_t)buffer;
}

} // namespace duckdb
