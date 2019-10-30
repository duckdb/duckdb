#include "storage/buffer_manager.hpp"

#include  "common/exception.hpp"

using namespace duckdb;
using namespace std;


BufferManager::BufferManager(FileSystem &fs, BlockManager &manager, string tmp, index_t maximum_memory) :
	fs(fs), manager(manager), current_memory(0), maximum_memory(maximum_memory), temp_directory(move(tmp)), temporary_id(MAXIMUM_BLOCK) {
	if (!temp_directory.empty()) {
		fs.CreateDirectory(temp_directory);
	}
}


BufferManager::~BufferManager() {
	if (!temp_directory.empty()) {
		fs.RemoveDirectory(temp_directory);
	}
}

unique_ptr<BlockHandle> BufferManager::Pin(block_id_t block_id) {
	// this method should only be used to pin blocks that exist in the file
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
		assert(buffer->type == FileBufferType::BLOCK);
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
	if (buffer->type == FileBufferType::BLOCK) {
		// block buffer: remove the block and reuse it
		auto block = (Block*) buffer;
		blocks.erase(block->id);
		// free up the memory
		current_memory -= BLOCK_SIZE;
		// finally return the block obtained from the current entry
		return unique_ptr_cast<FileBuffer, Block>(move(entry->buffer));
	} else {
		// managed buffer: cannot return a block here
		auto managed = (ManagedBuffer*) buffer;
		if (!managed->can_destroy) {
			// cannot destroy this buffer: write it to disk first so it can be reloaded later
			WriteTemporaryBuffer(*managed);
		}
		blocks.erase(managed->id);
		// free up the memory
		current_memory -= managed->size;
		return nullptr;
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

unique_ptr<ManagedBufferHandle> BufferManager::PinBuffer(block_id_t buffer_id, bool can_destroy) {
	assert(buffer_id >= MAXIMUM_BLOCK);
	lock_guard<mutex> lock(block_lock);
	// check if we have this buffer here
	auto entry = blocks.find(buffer_id);
	if (entry == blocks.end()) {
		if (can_destroy) {
			// buffer was destroyed: return nullptr
			return nullptr;
		} else {
			// buffer was unloaded but not destroyed: read from disk
			return ReadTemporaryBuffer(buffer_id);
		}
	}
	// we still have the buffer, add a reference to it
	auto buffer = entry->second->buffer.get();
	AddReference(entry->second);
	// now return it
	assert(buffer->type == FileBufferType::MANAGED_BUFFER);
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

string BufferManager::GetTemporaryPath(block_id_t id) {
	return fs.JoinPath(temp_directory, to_string(id) + ".block");
}

void BufferManager::WriteTemporaryBuffer(ManagedBuffer &buffer) {
	if (temp_directory.empty()) {
		throw Exception("Out-of-memory: cannot evict buffer because no temporary directory is specified!\nTo enable temporary buffer eviction set a temporary directory in the configuration");
	}
	// get the path to write to
	auto path = GetTemporaryPath(buffer.id);
	// create the file and write the size followed by the buffer contents
	auto handle = fs.OpenFile(path, FileFlags::WRITE | FileFlags::CREATE);
	handle->Write(&buffer.size, sizeof(index_t), 0);
	buffer.Write(*handle, sizeof(index_t));
}

unique_ptr<ManagedBufferHandle> BufferManager::ReadTemporaryBuffer(block_id_t id) {
	if (temp_directory.empty()) {
		throw Exception("Out-of-memory: cannot read buffer because no temporary directory is specified!\nTo enable temporary buffer eviction set a temporary directory in the configuration");
	}
	index_t alloc_size;
	// open the temporary file and read the size
	auto path = GetTemporaryPath(id);
	auto handle = fs.OpenFile(path, FileFlags::READ);
	handle->Read(&alloc_size, sizeof(index_t), 0);
	// first evict blocks until we can handle the size
	while(current_memory + alloc_size > maximum_memory) {
		EvictBlock();
	}
	// now allocate a buffer of this size and read the data into that buffer
	auto buffer = make_unique<ManagedBuffer>(*this, alloc_size, false, id);
	buffer->Read(*handle, sizeof(index_t));

	auto managed_buffer = buffer.get();
	current_memory += alloc_size;
	// create a new entry and append it to the used list
	auto buffer_entry = make_unique<BufferEntry>(move(buffer));
	blocks.insert(make_pair(id, buffer_entry.get()));
	used_list.Append(move(buffer_entry));
	// now return a handle to the entry
	return make_unique<ManagedBufferHandle>(*this, managed_buffer, id);
}