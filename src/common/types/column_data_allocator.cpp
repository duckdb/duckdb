#include "duckdb/common/types/column_data_allocator.hpp"

#include "duckdb/common/types/column_data_collection_segment.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

ColumnDataAllocator::ColumnDataAllocator(Allocator &allocator) : type(ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR) {
	alloc.allocator = &allocator;
}

ColumnDataAllocator::ColumnDataAllocator(BufferManager &buffer_manager)
    : type(ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR) {
	alloc.buffer_manager = &buffer_manager;
}

ColumnDataAllocator::ColumnDataAllocator(ClientContext &context, ColumnDataAllocatorType allocator_type)
    : type(allocator_type) {
	switch (type) {
	case ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR:
		alloc.buffer_manager = &BufferManager::GetBufferManager(context);
		break;
	case ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR:
		alloc.allocator = &Allocator::Get(context);
		break;
	default:
		throw InternalException("Unrecognized column data allocator type");
	}
}

BufferHandle ColumnDataAllocator::Pin(uint32_t block_id) {
	D_ASSERT(type == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
	shared_ptr<BlockHandle> handle;
	if (shared) {
		// we only need to grab the lock when accessing the vector, because vector access is not thread-safe:
		// the vector can be resized by another thread while we try to access it
		lock_guard<mutex> guard(lock);
		handle = blocks[block_id].handle;
	} else {
		handle = blocks[block_id].handle;
	}
	return alloc.buffer_manager->Pin(handle);
}

void ColumnDataAllocator::AllocateBlock() {
	D_ASSERT(type == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
	BlockMetaData data;
	data.size = 0;
	data.capacity = Storage::BLOCK_SIZE;
	data.handle = alloc.buffer_manager->RegisterMemory(Storage::BLOCK_SIZE, false);
	blocks.push_back(move(data));
}

void ColumnDataAllocator::AllocateEmptyBlock(idx_t size) {
	auto allocation_amount = MaxValue<idx_t>(NextPowerOfTwo(size), 4096);
	if (!blocks.empty()) {
		auto last_capacity = blocks.back().capacity;
		auto next_capacity = MinValue<idx_t>(last_capacity * 2, last_capacity + Storage::BLOCK_SIZE);
		allocation_amount = MaxValue<idx_t>(next_capacity, allocation_amount);
	}
	D_ASSERT(type == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR);
	BlockMetaData data;
	data.size = 0;
	data.capacity = allocation_amount;
	data.handle = nullptr;
	blocks.push_back(move(data));
}

void ColumnDataAllocator::AssignPointer(uint32_t &block_id, uint32_t &offset, data_ptr_t pointer) {
	auto pointer_value = uintptr_t(pointer);
	if (sizeof(uintptr_t) == sizeof(uint32_t)) {
		block_id = uint32_t(pointer_value);
	} else if (sizeof(uintptr_t) == sizeof(uint64_t)) {
		block_id = uint32_t(pointer_value & 0xFFFFFFFF);
		offset = uint32_t(pointer_value >> 32);
	} else {
		throw InternalException("ColumnDataCollection: Architecture not supported!?");
	}
}

void ColumnDataAllocator::AllocateBuffer(idx_t size, uint32_t &block_id, uint32_t &offset,
                                         ChunkManagementState *chunk_state) {
	D_ASSERT(allocated_data.empty());
	if (blocks.empty() || blocks.back().Capacity() < size) {
		AllocateBlock();
		if (chunk_state && !blocks.empty()) {
			auto &last_block = blocks.back();
			auto new_block_id = blocks.size() - 1;
			auto pinned_block = alloc.buffer_manager->Pin(last_block.handle);
			chunk_state->handles[new_block_id] = move(pinned_block);
		}
	}
	auto &block = blocks.back();
	D_ASSERT(size <= block.capacity - block.size);
	block_id = blocks.size() - 1;
	if (chunk_state && chunk_state->handles.find(block_id) == chunk_state->handles.end()) {
		// not guaranteed to be pinned already by this thread (if shared allocator)
		chunk_state->handles[block_id] = alloc.buffer_manager->Pin(blocks[block_id].handle);
	}
	offset = block.size;
	block.size += size;
}

void ColumnDataAllocator::AllocateMemory(idx_t size, uint32_t &block_id, uint32_t &offset,
                                         ChunkManagementState *chunk_state) {
	D_ASSERT(blocks.size() == allocated_data.size());
	if (blocks.empty() || blocks.back().Capacity() < size) {
		AllocateEmptyBlock(size);
		auto &last_block = blocks.back();
		auto allocated = alloc.allocator->Allocate(last_block.capacity);
		allocated_data.push_back(move(allocated));
	}
	auto &block = blocks.back();
	D_ASSERT(size <= block.capacity - block.size);
	AssignPointer(block_id, offset, allocated_data.back().get() + block.size);
	block.size += size;
}

void ColumnDataAllocator::AllocateData(idx_t size, uint32_t &block_id, uint32_t &offset,
                                       ChunkManagementState *chunk_state) {
	switch (type) {
	case ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR:
		if (shared) {
			lock_guard<mutex> guard(lock);
			AllocateBuffer(size, block_id, offset, chunk_state);
		} else {
			AllocateBuffer(size, block_id, offset, chunk_state);
		}
		break;
	case ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR:
		D_ASSERT(!shared);
		AllocateMemory(size, block_id, offset, chunk_state);
		break;
	default:
		throw InternalException("Unrecognized allocator type");
	}
}

void ColumnDataAllocator::Initialize(ColumnDataAllocator &other) {
	D_ASSERT(other.HasBlocks());
	blocks.push_back(other.blocks.back());
}

data_ptr_t ColumnDataAllocator::GetDataPointer(ChunkManagementState &state, uint32_t block_id, uint32_t offset) {
	if (type == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR) {
		// in-memory allocator: construct pointer from block_id and offset
		if (sizeof(uintptr_t) == sizeof(uint32_t)) {
			uintptr_t pointer_value = uintptr_t(block_id);
			return (data_ptr_t)pointer_value;
		} else if (sizeof(uintptr_t) == sizeof(uint64_t)) {
			uintptr_t pointer_value = (uintptr_t(offset) << 32) | uintptr_t(block_id);
			return (data_ptr_t)pointer_value;
		} else {
			throw InternalException("ColumnDataCollection: Architecture not supported!?");
		}
	}
	D_ASSERT(state.handles.find(block_id) != state.handles.end());
	return state.handles[block_id].Ptr() + offset;
}

ColumnDataAllocator::SwizzleCallback::SwizzleCallback(ColumnDataAllocator *allocator, VectorMetaData &parent_vdata,
                                                      idx_t entry_offset, VectorMetaData &child_vdata, idx_t count,
                                                      bool swizzle)
    : allocator(allocator), parent_block_id(parent_vdata.block_id), parent_block_offset(parent_vdata.offset),
      child_block_id(child_vdata.block_id), child_block_offset(child_vdata.offset), entry_offset(entry_offset),
      count(count), swizzle(swizzle) {
}

template <bool SWIZZLE>
static void SwizzleLoop(string_t *entries, const ValidityMask &validity, const idx_t &entry_offset,
                        char *const heap_ptr, const idx_t &count) {
	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(entry_offset + i)) {
			continue;
		}
		auto &entry = entries[entry_offset + i];
		if (entry.IsInlined()) {
			continue;
		}

		if (SWIZZLE) {
			entry.Swizzle(heap_ptr);
		} else {
			entry.Unswizzle(heap_ptr);
#ifdef DEBUG
			entry.Verify();
#endif
		}
	}
}

void ColumnDataAllocator::SwizzleCallback::Operation(FileBuffer &file_buffer) {
	// file_buffer is the StringHeap block
	auto heap_ptr = file_buffer.buffer + child_block_offset;

	data_ptr_t base_ptr;
	ChunkManagementState state;
	if (parent_block_id == child_block_id) {
		base_ptr = file_buffer.buffer + parent_block_offset;
	} else {
		// lock of the BlockHandle is already held during the callback,
		// so we have to make sure we don't Pin() the same block else we deadlock
		state.handles[parent_block_id] = allocator->Pin(parent_block_id);
		base_ptr = allocator->GetDataPointer(state, parent_block_id, parent_block_offset);
	}

	auto entries = (string_t *)base_ptr;
	auto validity_data = ColumnDataCollectionSegment::GetValidityPointer(base_ptr, sizeof(string_t));
	ValidityMask validity(validity_data);

	if (swizzle) {
		SwizzleLoop<true>(entries, validity, entry_offset, (char *)heap_ptr, count);
	} else {
		SwizzleLoop<false>(entries, validity, entry_offset, (char *)heap_ptr, count);
	}
}

void ColumnDataAllocator::AddSwizzleCallbacks(VectorMetaData &parent_vdata, idx_t entry_offset,
                                              VectorMetaData &child_vdata, idx_t count) {
	D_ASSERT(type == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
	D_ASSERT(count != 0);
	auto &heap_block = blocks[child_vdata.block_id];

	heap_block.handle->AddUnloadCallback(
	    make_unique<SwizzleCallback>(this, parent_vdata, entry_offset, child_vdata, count, true));
	heap_block.handle->AddLoadCallback(
	    make_unique<SwizzleCallback>(this, parent_vdata, entry_offset, child_vdata, count, false));
}

void ColumnDataAllocator::DeleteBlock(uint32_t block_id) {
	blocks[block_id].handle->SetCanDestroy(true);
}

Allocator &ColumnDataAllocator::GetAllocator() {
	return type == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR ? *alloc.allocator
	                                                            : alloc.buffer_manager->GetBufferAllocator();
}

void ColumnDataAllocator::InitializeChunkState(ChunkManagementState &state, ChunkMetaData &chunk) {
	if (type != ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR) {
		// nothing to pin
		return;
	}
	// release any handles that are no longer required
	bool found_handle;
	do {
		found_handle = false;
		for (auto it = state.handles.begin(); it != state.handles.end(); it++) {
			if (chunk.block_ids.find(it->first) != chunk.block_ids.end()) {
				// still required: do not release
				continue;
			}
			state.handles.erase(it);
			found_handle = true;
			break;
		}
	} while (found_handle);

	// grab any handles that are now required
	for (auto &block_id : chunk.block_ids) {
		if (state.handles.find(block_id) != state.handles.end()) {
			// already pinned: don't need to do anything
			continue;
		}
		state.handles[block_id] = Pin(block_id);
	}
}

uint32_t BlockMetaData::Capacity() {
	D_ASSERT(size <= capacity);
	return capacity - size;
}

} // namespace duckdb
