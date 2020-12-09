#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "concurrentqueue.h"

namespace duckdb {
using namespace std;

BlockHandle::BlockHandle(BufferManager &manager_p, block_id_t block_id_p) :
	manager(manager_p) {
	block_id = block_id_p;
	readers = 0;
	buffer = nullptr;
	eviction_timestamp = 0;
	state = BlockState::BLOCK_UNLOADED;
	can_destroy = false;
	memory_usage = Storage::BLOCK_ALLOC_SIZE;
}

BlockHandle::BlockHandle(BufferManager &manager_p, block_id_t block_id_p, unique_ptr<FileBuffer> buffer_p, bool can_destroy_p, idx_t alloc_size) :
	manager(manager_p) {
	D_ASSERT(alloc_size >= Storage::BLOCK_SIZE);
	block_id = block_id_p;
	readers = 0;
	buffer = move(buffer_p);
	eviction_timestamp = 0;
	state = BlockState::BLOCK_LOADED;
	can_destroy = can_destroy_p;
	memory_usage = alloc_size;
}

BlockHandle::~BlockHandle() {
	// no references remain to this block: erase
	if (state == BlockState::BLOCK_LOADED) {
		// the block is still loaded in memory: erase it
		buffer.reset();
		manager.current_memory -= memory_usage;
	}
	manager.UnregisterBlock(block_id, can_destroy);
}

unique_ptr<BufferHandle> BlockHandle::Load(shared_ptr<BlockHandle> &handle) {
	if (handle->state == BlockState::BLOCK_LOADED) {
		// already loaded
		D_ASSERT(handle->buffer);
		return make_unique<BufferHandle>(handle->manager, handle, handle->buffer.get());
	}
	handle->state = BlockState::BLOCK_LOADED;
	if (handle->block_id < MAXIMUM_BLOCK) {
		auto block = make_unique<Block>(handle->block_id);
		handle->manager.manager.Read(*block);
		handle->buffer = move(block);
	} else {
		if (handle->can_destroy) {
			return nullptr;
		} else {
			handle->buffer = handle->manager.ReadTemporaryBuffer(handle->block_id);
		}
	}
	return make_unique<BufferHandle>(handle->manager, handle, handle->buffer.get());
}

void BlockHandle::Unload() {
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded: nothing to do
		return;
	}
	D_ASSERT(CanUnload());
	D_ASSERT(memory_usage >= Storage::BLOCK_SIZE);
	state = BlockState::BLOCK_UNLOADED;
	if (block_id >= MAXIMUM_BLOCK && !can_destroy) {
		// temporary block that cannot be destroyed: write to temporary file
		manager.WriteTemporaryBuffer((ManagedBuffer &) *buffer);
	}
	buffer.reset();
	manager.current_memory -= memory_usage;
}

bool BlockHandle::CanUnload() {
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded
		return false;
	}
	if (readers > 0) {
		// there are active readers
		return false;
	}
	if (block_id >= MAXIMUM_BLOCK && !can_destroy && manager.temp_directory.empty()) {
		// in order to unload this block we need to write it to a temporary buffer
		// however, no temporary directory is specified!
		// hence we cannot unload the block
		return false;
	}
	return true;
}


struct BufferEvictionNode {
	BufferEvictionNode(std::weak_ptr<BlockHandle> handle_p, idx_t timestamp_p) :
		handle(move(handle_p)), timestamp(timestamp_p) {
		D_ASSERT(!handle.expired());
	}

	std::weak_ptr<BlockHandle> handle;
	idx_t timestamp;

	bool CanUnload(BlockHandle &handle) {
		if (timestamp != handle.eviction_timestamp) {
			// handle was used in between
			return false;
		}
		return handle.CanUnload();
	}
};

typedef moodycamel::ConcurrentQueue<unique_ptr<BufferEvictionNode>> eviction_queue_t;

struct EvictionQueue {
	eviction_queue_t q;
};

BufferManager::BufferManager(FileSystem &fs, BlockManager &manager, string tmp, idx_t maximum_memory)
    : fs(fs), manager(manager), current_memory(0), maximum_memory(maximum_memory), temp_directory(move(tmp)),
      queue(make_unique<EvictionQueue>()), temporary_id(MAXIMUM_BLOCK) {
	if (!temp_directory.empty()) {
		fs.CreateDirectory(temp_directory);
	}
}

BufferManager::~BufferManager() {
	if (!temp_directory.empty()) {
		fs.RemoveDirectory(temp_directory);
	}
}

shared_ptr<BlockHandle> BufferManager::RegisterBlock(block_id_t block_id) {
	lock_guard<mutex> lock(manager_lock);
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
	// create a new block pointer for this block
	auto result = make_shared<BlockHandle>(*this, block_id);
	// register the block pointer in the set of blocks as a weak pointer
	blocks[block_id] = weak_ptr<BlockHandle>(result);
	return result;
}

shared_ptr<BlockHandle> BufferManager::RegisterMemory(idx_t alloc_size, bool can_destroy) {
	lock_guard<mutex> lock(manager_lock);
	// first evict blocks until we have enough memory to store this buffer
	EvictBlocks(alloc_size, maximum_memory);

	// allocate the buffer
	auto temp_id = ++temporary_id;
	auto buffer = make_unique<ManagedBuffer>(*this, alloc_size, can_destroy, temp_id);

	// create a new block pointer for this block
	return make_shared<BlockHandle>(*this, temp_id, move(buffer), can_destroy, alloc_size);
}

unique_ptr<BufferHandle> BufferManager::Allocate(idx_t alloc_size) {
	auto block = RegisterMemory(alloc_size, true);
	return Pin(block);
}

unique_ptr<BufferHandle> BufferManager::Pin(shared_ptr<BlockHandle> &handle) {
	// lock the block
	lock_guard<mutex> lock(handle->lock);
	// check if the block is already loaded
	if (handle->state == BlockState::BLOCK_LOADED) {
		// the block is loaded, increment the reader count and return a pointer to the handle
		handle->readers++;
		return handle->Load(handle);
	}
	lock_guard<mutex> buffer_lock(manager_lock);
	// evict blocks until we have space for the current block
	EvictBlocks(handle->memory_usage, maximum_memory);
	// now we can actually load the current block
	D_ASSERT(handle->readers == 0);
	handle->readers = 1;
	return handle->Load(handle);
}

void BufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->readers > 0);
	handle->readers--;
	handle->eviction_timestamp++;
	queue->q.enqueue(make_unique<BufferEvictionNode>(weak_ptr<BlockHandle>(handle), handle->eviction_timestamp));
	// FIXME: do some house-keeping to prevent the queue from being flooded with many old blocks
}

void BufferManager::EvictBlocks(idx_t extra_memory, idx_t memory_limit) {
	unique_ptr<BufferEvictionNode> node;
	while(current_memory + extra_memory > memory_limit) {
		// get a block to unpin from the queue
		if (!queue->q.try_dequeue(node)) {
			throw Exception("Not enough memory to complete operation!");
		}
		// get a reference to the underlying block pointer
		auto handle = node->handle.lock();
		if (!handle) {
			continue;
		}
		if (!node->CanUnload(*handle)) {
			// early out: we already know that we cannot unload this node
			continue;
		}
		// we might be able to free this block: grab the mutex and check if we can free it
		lock_guard<mutex> lock(handle->lock);
		if (!node->CanUnload(*handle)) {
			// something changed in the mean-time, bail out
			continue;
		}
		// hooray, we can unload the block
		// release the memory and mark the block as unloaded
		handle->Unload();
	}
	current_memory += extra_memory;
}

void BufferManager::UnregisterBlock(block_id_t block_id, bool can_destroy) {
	lock_guard<mutex> lock(manager_lock);
	if (block_id >= MAXIMUM_BLOCK) {
		// in-memory buffer: destroy the buffer
		if (!can_destroy) {
			// buffer could have been offloaded to disk: remove the file
			DeleteTemporaryFile(block_id);
		}
	} else {
		// on-disk block: erase from list of blocks in manager
		blocks.erase(block_id);
	}
}
void BufferManager::SetLimit(idx_t limit) {
	lock_guard<mutex> buffer_lock(manager_lock);
	EvictBlocks(0, limit);
	maximum_memory = limit;
}

string BufferManager::GetTemporaryPath(block_id_t id) {
	return fs.JoinPath(temp_directory, to_string(id) + ".block");
}

void BufferManager::WriteTemporaryBuffer(ManagedBuffer &buffer) {
	D_ASSERT(!temp_directory.empty());
	D_ASSERT(buffer.size + Storage::BLOCK_HEADER_SIZE >= Storage::BLOCK_ALLOC_SIZE);
	// get the path to write to
	auto path = GetTemporaryPath(buffer.id);
	// create the file and write the size followed by the buffer contents
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	handle->Write(&buffer.size, sizeof(idx_t), 0);
	buffer.Write(*handle, sizeof(idx_t));
}

unique_ptr<FileBuffer> BufferManager::ReadTemporaryBuffer(block_id_t id) {
	if (temp_directory.empty()) {
		throw Exception("Out-of-memory: cannot read buffer because no temporary directory is specified!\nTo enable "
		                "temporary buffer eviction set a temporary directory in the configuration");
	}
	idx_t alloc_size;
	// open the temporary file and read the size
	auto path = GetTemporaryPath(id);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	handle->Read(&alloc_size, sizeof(idx_t), 0);

	// now allocate a buffer of this size and read the data into that buffer
	auto buffer = make_unique<ManagedBuffer>(*this, alloc_size + Storage::BLOCK_HEADER_SIZE, false, id);
	buffer->Read(*handle, sizeof(idx_t));
	return buffer;
}

void BufferManager::DeleteTemporaryFile(block_id_t id) {
	auto path = GetTemporaryPath(id);
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}

} // namespace duckdb
