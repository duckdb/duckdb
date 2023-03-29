#include "duckdb/storage/cbuffer_manager.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/common/external_file_buffer.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer/dummy_buffer_pool.hpp"
#include "duckdb/storage/buffer/custom_block_handle.hpp"

namespace duckdb {

shared_ptr<BlockHandle> CustomInMemoryBlockManager::RegisterBlock(block_id_t block_id, bool is_meta_block) {
	lock_guard<mutex> lock(blocks_lock);
	// check if the block already exists
	auto entry = blocks.find(block_id);
	if (entry != blocks.end()) {
		// already exists: check if it hasn't expired yet
		auto existing_ptr = entry->second.lock();
		if (existing_ptr) {
			//! it hasn't! return it
			return existing_ptr;
		}
	}
	//FIXME: can we just use CBufferManager::RegisterSmallMemory here?
	auto alloc_size = Storage::BLOCK_ALLOC_SIZE;

	// create a new block pointer for this block
	auto& cbuffer_manager = (CBufferManager&)buffer_manager;
	auto &config = cbuffer_manager.config;
	auto block_handle = config.allocate_func(config.data, Storage::BLOCK_ALLOC_SIZE);
	auto allocation = (data_ptr_t)config.pin_func(config.data, block_handle);

	// Used to manage the used_memory counter with RAII
	BufferPoolReservation reservation(cbuffer_manager.GetBufferPool());
	reservation.size = alloc_size;

	unique_ptr<FileBuffer> buffer = make_unique<ExternalFileBuffer>(allocation, alloc_size);
	shared_ptr<BlockHandle> result = make_shared<CustomBlockHandle>(block_handle, config, *this, block_id, std::move(buffer),
	                                           false, alloc_size, std::move(reservation));
	// for meta block, cache the handle in meta_blocks
	if (is_meta_block) {
		meta_blocks[block_id] = result;
	}
	// register the block pointer in the set of blocks as a weak pointer
	blocks[block_id] = weak_ptr<BlockHandle>(result);
	return result;
}

Allocator &CBufferManager::GetBufferAllocator() {
	return allocator;
}

CBufferManager::CBufferManager(CBufferManagerConfig config_p) : BufferManager(), config(config_p) {
	block_manager = make_unique<CustomInMemoryBlockManager>(*this);
	buffer_pool = make_unique<DummyBufferPool>();
}

BufferHandle CBufferManager::Allocate(idx_t block_size, bool can_destroy, shared_ptr<BlockHandle> *block) {
	idx_t alloc_size = BufferManager::GetAllocSize(block_size);
	shared_ptr<BlockHandle> temp_block; // Doesn't this cause a memory-leak, or at the very least heap-use-after-free???
	shared_ptr<BlockHandle> *handle_p = block ? block : &temp_block;

	// Used to manage the used_memory counter with RAII
	BufferPoolReservation reservation(this->GetBufferPool());
	reservation.size = alloc_size;

	auto custom_handle = config.allocate_func(config.data, alloc_size);
	auto pinned_allocation = (data_ptr_t)config.pin_func(config.data, custom_handle);
	unique_ptr<FileBuffer> buffer = make_unique<ExternalFileBuffer>(pinned_allocation, alloc_size);
	*handle_p = make_shared<CustomBlockHandle>(custom_handle, config, *block_manager, ++temporary_id, std::move(buffer),
	                                           can_destroy, alloc_size, std::move(reservation));

	return Pin(*handle_p);
}

BufferPool &CBufferManager::GetBufferPool() {
	return *buffer_pool;
}

shared_ptr<BlockHandle> CBufferManager::RegisterSmallMemory(idx_t block_size) {
	idx_t alloc_size = BufferManager::GetAllocSize(block_size);
	// create a new block pointer for this block

	// Used to manage the used_memory counter with RAII
	BufferPoolReservation reservation(this->GetBufferPool());
	reservation.size = alloc_size;

	auto custom_handle = config.allocate_func(config.data, alloc_size);
	auto pinned_allocation = (data_ptr_t)config.pin_func(config.data, custom_handle);
	unique_ptr<FileBuffer> buffer = make_unique<ExternalFileBuffer>(pinned_allocation, alloc_size);
	return make_shared<CustomBlockHandle>(custom_handle, config, *block_manager, ++temporary_id, std::move(buffer),
	                                      false, alloc_size, std::move(reservation));
}

void CBufferManager::ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->state == BlockState::BLOCK_LOADED);
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
	D_ASSERT(handle->memory_usage == handle->memory_charge.size);

	// FIXME: shouldn't we assert that it's unpinned?
	// there could be readers who depend on the old allocation

	auto req = handle->buffer->CalculateMemory(block_size);
	int64_t memory_delta = (int64_t)req.alloc_size - handle->memory_usage;

	handle->memory_charge.Resize(req.alloc_size);
	handle->ResizeBuffer(block_size, memory_delta);
}

BufferHandle CBufferManager::Pin(shared_ptr<BlockHandle> &handle) {
	idx_t required_memory;
	{
		// lock the block
		lock_guard<mutex> lock(handle->lock);
		// check if the block is already loaded
		if (handle->state == BlockState::BLOCK_LOADED) {
			if (handle->readers == 0) {
				auto &custom_handle = (CustomBlockHandle &)*handle;
				// Call pin again to mark that we're using the buffer again
				(void)config.pin_func(config.data, custom_handle.block);
			}
			handle->readers++;
			return handle->Load(handle);
		}
		required_memory = handle->memory_usage;
	}
	// Load the block, setting the allocation
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->readers == 0);
	handle->readers = 1;

	auto &custom_handle = (CustomBlockHandle &)*handle;
	auto allocation = (data_ptr_t)config.pin_func(config.data, custom_handle.block);
	unique_ptr<FileBuffer> new_buffer = make_unique<ExternalFileBuffer>(allocation, required_memory);
	TempBufferPoolReservation reservation(*buffer_pool, required_memory);
	handle->memory_charge = std::move(reservation);

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
	D_ASSERT(handle->readers > 0);
	handle->readers--;
	auto &custom_handle = (CustomBlockHandle &)*handle;
	auto user_buffer = custom_handle.block;
	if (handle->readers == 0) {
		config.unpin_func(config.data, user_buffer);
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

void CBufferManager::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	// no op
}

////===--------------------------------------------------------------------===//
//// Buffer Allocator
////===--------------------------------------------------------------------===//
// data_ptr_t CBufferManager::CBufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
//	auto &data = (CBufferAllocatorData &)*private_data;
//	auto &config = data.manager.config;
//	duckdb_block buffer = config.allocate_func(config.data, size);
//	return (data_ptr_t)buffer;
//}

// void CBufferManager::CBufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
//	auto &data = (CBufferAllocatorData &)*private_data;
//	auto &config = data.manager.config;
//	config.destroy_func(config.data, pointer);
//}

// data_ptr_t CBufferManager::CBufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer,
//                                                   idx_t old_size, idx_t size) {
//	auto &data = (CBufferAllocatorData &)*private_data;
//	auto &config = data.manager.config;
//	auto buffer = config.reallocate_func(config.data, pointer, old_size, size);
//	return (data_ptr_t)buffer;
//}

} // namespace duckdb
