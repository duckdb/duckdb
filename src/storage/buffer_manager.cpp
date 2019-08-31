#include "storage/buffer_manager.hpp"

#include  "common/exception.hpp"

using namespace duckdb;
using namespace std;


BufferManager::BufferManager(BlockManager &manager, index_t maximum_memory) :
	current_memory(0), maximum_memory(maximum_memory), manager(manager), temporary_id(MAXIMUM_BLOCK) {

}

unique_ptr<BlockHandle> BufferManager::Pin(block_id_t block_id) {
	// this method should only be used to pin blocks
	assert(block_id < MAXIMUM_BLOCK);

	// first obtain a lock on the set of blocks
	lock_guard<mutex> lock(block_lock);
	// now check if the block is already loaded
	Block *result_block;
	auto entry = blocks.find(block_id);
	if (entry == blocks.end()) {
		// block is not loaded, load the block
		current_memory += BLOCK_SIZE;
		unique_ptr<Block> block;
		if (current_memory > maximum_memory) {
			// not enough memory to hold the block: have to evict a block first
			block = EvictBlock();
			if (!block) {
				// evicted a managed buffer: no block returned
				// create a new block
				block = make_unique<Block>(block_id);
			} else {
				// take over the evicted block and use it to hold this block
				block->id = block_id;
			}
		} else {
			// enough memory to create a new block: allocate it
			block = make_unique<Block>(block_id);
		}
		manager.Read(*block);
		result_block = block.get();
		// create a new buffer entry for this block and insert it into the block list
		auto buffer_entry = make_unique<BufferEntry>(move(block));
		blocks.insert(make_pair(block_id, buffer_entry.get()));
		used_list.Append(move(buffer_entry));
	} else {
		auto buffer = entry->second->buffer.get();
		assert(buffer->type == BufferType::FILE_BUFFER);
		result_block = (Block*) buffer;
		// add one to the reference count
		AddReference(entry->second);
	}
	return make_unique<BlockHandle>(*this, result_block, block_id);
}

void BufferManager::AddReference(BufferEntry *entry) {
	entry->ref_count++;
	if (entry->ref_count == 1) {
		// ref count is 1, that means it used to be 0 (unused)
		// move from lru to used_list
		auto current_entry = lru.Erase(entry);
		used_list.Append(move(current_entry));
	}
}

unique_ptr<ManagedBufferHandle> BufferManager::Allocate(index_t alloc_size, bool can_destroy) {
	lock_guard<mutex> lock(block_lock);
	// first evict blocks until we have enough memory to store this buffer
	while(current_memory + alloc_size > maximum_memory) {
		EvictBlock();
	}
	// now allocate the buffer with a new temporary id
	auto temp_id = ++temporary_id;
	auto buffer = make_unique<ManagedBuffer>(*this, alloc_size, can_destroy, temp_id);
	auto managed_buffer = buffer.get();
	current_memory += alloc_size;
	// create a new entry and append it to the used list
	auto buffer_entry = make_unique<BufferEntry>(move(buffer));
	blocks.insert(make_pair(temp_id, buffer_entry.get()));
	used_list.Append(move(buffer_entry));
	// now return a handle to the entry
	return make_unique<ManagedBufferHandle>(*this, managed_buffer, temp_id);
}

unique_ptr<ManagedBufferHandle> BufferManager::PinBuffer(block_id_t buffer_id) {
	assert(buffer_id >= MAXIMUM_BLOCK);
	lock_guard<mutex> lock(block_lock);
	// check if we have this buffer here
	auto entry = blocks.find(buffer_id);
	if (entry == blocks.end()) {
		// buffer was already destroyed: return nullptr
		return nullptr;
	}
	// we still have the buffer, add a reference to it
	auto buffer = entry->second->buffer.get();
	AddReference(entry->second);
	// now return it
	assert(buffer->type == BufferType::MANAGED_BUFFER);
	auto managed = (ManagedBuffer*) buffer;
	assert(managed->id == buffer_id);
	return make_unique<ManagedBufferHandle>(*this, managed, buffer_id);
}


void BufferManager::DestroyBuffer(block_id_t buffer_id) {
	assert(buffer_id >= MAXIMUM_BLOCK);
	// this is like unpin, except we just destroy the entry entirely instead of adding it to the LRU list
	// first find the block in the set of blocks
	auto entry = blocks.find(buffer_id);
	assert(entry != blocks.end());
	blocks.erase(buffer_id);
	used_list.Erase(entry->second);
}

void BufferManager::Unpin(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	// first find the block in the set of blocks
	auto entry = blocks.find(block_id);
	assert(entry != blocks.end());

	auto buffer_entry = entry->second;
	// then decerase the ref count
	assert(buffer_entry->ref_count > 0);
	buffer_entry->ref_count--;
	if (buffer_entry->ref_count == 0) {
		// no references left: move block out of used list and into lru list
		auto entry = used_list.Erase(buffer_entry);
		lru.Append(move(entry));
	}
}

unique_ptr<Block> BufferManager::EvictBlock() {
	// pop the first entry from the lru list
	auto entry = lru.Pop();
	if (!entry) {
		throw Exception("Not enough memory to complete operation!");
	}
	assert(entry->ref_count == 0);
	// erase this identifier from the set of blocks
	auto buffer = entry->buffer.get();
	if (buffer->type == BufferType::FILE_BUFFER) {
		// block buffer: remove the block and reuse it
		auto block = (Block*) buffer;
		blocks.erase(block->id);
		// free up the memory
		current_memory -= BLOCK_SIZE;
		// finally return the block obtained from the current entry
		return unique_ptr_cast<Buffer, Block>(move(entry->buffer));
	} else {
		// managed buffer: cannot return a block here
		auto managed = (ManagedBuffer*) buffer;
		if (!managed->can_destroy) {
			throw Exception("FIXME: cannot destroy this managed buffer yet! need to write to temporary file");
		}
		blocks.erase(managed->id);
		// free up the memory
		current_memory -= managed->size;
		return nullptr;
	}
}
