#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

BlockHandle::BlockHandle(DatabaseInstance &db, block_id_t block_id_p)
    : db(db), readers(0), block_id(block_id_p), buffer(nullptr), eviction_timestamp(0), can_destroy(false) {
	eviction_timestamp = 0;
	state = BlockState::BLOCK_UNLOADED;
	memory_usage = Storage::BLOCK_ALLOC_SIZE;
}

BlockHandle::BlockHandle(DatabaseInstance &db, block_id_t block_id_p, unique_ptr<FileBuffer> buffer_p,
                         bool can_destroy_p, idx_t block_size)
    : db(db), readers(0), block_id(block_id_p), eviction_timestamp(0), can_destroy(can_destroy_p) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	buffer = move(buffer_p);
	state = BlockState::BLOCK_LOADED;
	memory_usage = block_size + Storage::BLOCK_HEADER_SIZE;
}

BlockHandle::~BlockHandle() {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	// no references remain to this block: erase
	if (state == BlockState::BLOCK_LOADED) {
		// the block is still loaded in memory: erase it
		buffer.reset();
		buffer_manager.current_memory -= memory_usage;
	}
	buffer_manager.UnregisterBlock(block_id, can_destroy);
}

unique_ptr<BufferHandle> BlockHandle::Load(shared_ptr<BlockHandle> &handle) {
	if (handle->state == BlockState::BLOCK_LOADED) {
		// already loaded
		D_ASSERT(handle->buffer);
		return make_unique<BufferHandle>(handle, handle->buffer.get());
	}

	auto &buffer_manager = BufferManager::GetBufferManager(handle->db);
	auto &block_manager = BlockManager::GetBlockManager(handle->db);
	if (handle->block_id < MAXIMUM_BLOCK) {
		auto block = make_unique<Block>(Allocator::Get(handle->db), handle->block_id);
		block_manager.Read(*block);
		handle->buffer = move(block);
	} else {
		if (handle->can_destroy) {
			return nullptr;
		} else {
			handle->buffer = buffer_manager.ReadTemporaryBuffer(handle->block_id);
		}
	}
	handle->state = BlockState::BLOCK_LOADED;
	return make_unique<BufferHandle>(handle, handle->buffer.get());
}

void BlockHandle::Unload() {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded: nothing to do
		return;
	}
	D_ASSERT(CanUnload());
	D_ASSERT(memory_usage >= Storage::BLOCK_ALLOC_SIZE);

	if (block_id >= MAXIMUM_BLOCK && !can_destroy) {
		// temporary block that cannot be destroyed: write to temporary file
		buffer_manager.WriteTemporaryBuffer((ManagedBuffer &)*buffer);
	}
	buffer.reset();
	buffer_manager.current_memory -= memory_usage;
	state = BlockState::BLOCK_UNLOADED;
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
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (block_id >= MAXIMUM_BLOCK && !can_destroy && buffer_manager.temp_directory.empty()) {
		// in order to unload this block we need to write it to a temporary buffer
		// however, no temporary directory is specified!
		// hence we cannot unload the block
		return false;
	}
	return true;
}

struct BufferEvictionNode {
	BufferEvictionNode(weak_ptr<BlockHandle> handle_p, idx_t timestamp_p)
	    : handle(move(handle_p)), timestamp(timestamp_p) {
		D_ASSERT(!handle.expired());
	}

	weak_ptr<BlockHandle> handle;
	idx_t timestamp;

	bool CanUnload(BlockHandle &handle_p) {
		if (timestamp != handle_p.eviction_timestamp) {
			// handle was used in between
			return false;
		}
		return handle_p.CanUnload();
	}

	shared_ptr<BlockHandle> TryGetBlockHandle() {
		auto handle_p = handle.lock();
		if (!handle_p) {
			// BlockHandle has been destroyed
			return nullptr;
		}
		if (!CanUnload(*handle_p)) {
			// handle was used in between
			return nullptr;
		}
		// this is the latest node in the queue with this handle
		return handle_p;
	}
};

typedef duckdb_moodycamel::ConcurrentQueue<unique_ptr<BufferEvictionNode>> eviction_queue_t;

struct EvictionQueue {
	eviction_queue_t q;
};

class TemporaryDirectoryHandle {
public:
	TemporaryDirectoryHandle(DatabaseInstance &db, string path_p) : db(db), temp_directory(move(path_p)) {
		auto &fs = FileSystem::GetFileSystem(db);
		if (!temp_directory.empty()) {
			fs.CreateDirectory(temp_directory);
		}
	}
	~TemporaryDirectoryHandle() {
		auto &fs = FileSystem::GetFileSystem(db);
		if (!temp_directory.empty()) {
			fs.RemoveDirectory(temp_directory);
		}
	}

private:
	DatabaseInstance &db;
	string temp_directory;
};

void BufferManager::SetTemporaryDirectory(string new_dir) {
	if (temp_directory_handle) {
		throw NotImplementedException("Cannot switch temporary directory after the current one has been used");
	}
	this->temp_directory = move(new_dir);
}

BufferManager::BufferManager(DatabaseInstance &db, string tmp, idx_t maximum_memory)
    : db(db), current_memory(0), maximum_memory(maximum_memory), temp_directory(move(tmp)),
      queue(make_unique<EvictionQueue>()), temporary_id(MAXIMUM_BLOCK) {
}

BufferManager::~BufferManager() {
}

shared_ptr<BlockHandle> BufferManager::RegisterBlock(block_id_t block_id) {
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
	// create a new block pointer for this block
	auto result = make_shared<BlockHandle>(db, block_id);
	// register the block pointer in the set of blocks as a weak pointer
	blocks[block_id] = weak_ptr<BlockHandle>(result);
	return result;
}

shared_ptr<BlockHandle> BufferManager::ConvertToPersistent(BlockManager &block_manager, block_id_t block_id,
                                                           shared_ptr<BlockHandle> old_block) {

	// pin the old block to ensure we have it loaded in memory
	auto old_handle = Pin(old_block);
	D_ASSERT(old_block->state == BlockState::BLOCK_LOADED);
	D_ASSERT(old_block->buffer);

	// register a block with the new block id
	auto new_block = RegisterBlock(block_id);
	D_ASSERT(new_block->state == BlockState::BLOCK_UNLOADED);
	D_ASSERT(new_block->readers == 0);

#ifdef DEBUG
	lock_guard<mutex> b_lock(blocks_lock);
#endif

	// move the data from the old block into data for the new block
	new_block->state = BlockState::BLOCK_LOADED;
	new_block->buffer = make_unique<Block>(*old_block->buffer, block_id);

	// clear the old buffer and unload it
	old_handle.reset();
	old_block->buffer.reset();
	old_block->state = BlockState::BLOCK_UNLOADED;
	old_block->memory_usage = 0;
	old_block.reset();

	// persist the new block to disk
	block_manager.Write(*new_block->buffer, block_id);

	AddToEvictionQueue(new_block);

	return new_block;
}

shared_ptr<BlockHandle> BufferManager::RegisterMemory(idx_t block_size, bool can_destroy) {
	auto alloc_size = block_size + Storage::BLOCK_HEADER_SIZE;
	// first evict blocks until we have enough memory to store this buffer
	if (!EvictBlocks(alloc_size, maximum_memory)) {
		throw OutOfMemoryException("could not allocate block of %lld bytes%s", alloc_size, InMemoryWarning());
	}

	// allocate the buffer
	auto temp_id = ++temporary_id;
	auto buffer = make_unique<ManagedBuffer>(db, block_size, can_destroy, temp_id);

	// create a new block pointer for this block
	return make_shared<BlockHandle>(db, temp_id, move(buffer), can_destroy, block_size);
}

unique_ptr<BufferHandle> BufferManager::Allocate(idx_t block_size) {
	auto block = RegisterMemory(block_size, true);
	return Pin(block);
}

void BufferManager::ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->state == BlockState::BLOCK_LOADED);
	auto alloc_size = block_size + Storage::BLOCK_HEADER_SIZE;
	int64_t required_memory = alloc_size - handle->memory_usage;
	if (required_memory == 0) {
		return;
	} else if (required_memory > 0) {
		// evict blocks until we have space to resize this block
		if (!EvictBlocks(required_memory, maximum_memory)) {
			throw OutOfMemoryException("failed to resize block from %lld to %lld%s", handle->memory_usage, alloc_size,
			                           InMemoryWarning());
		}
	} else {
		// no need to evict blocks
		current_memory -= idx_t(-required_memory);
	}

	// resize and adjust current memory
	handle->buffer->Resize(block_size);
	handle->memory_usage = alloc_size;
}

unique_ptr<BufferHandle> BufferManager::Pin(shared_ptr<BlockHandle> &handle) {
	idx_t required_memory;
	{
		// lock the block
		lock_guard<mutex> lock(handle->lock);
		// check if the block is already loaded
		if (handle->state == BlockState::BLOCK_LOADED) {
			// the block is loaded, increment the reader count and return a pointer to the handle
			handle->readers++;
			return handle->Load(handle);
		}
		required_memory = handle->memory_usage;
	}
	// evict blocks until we have space for the current block
	if (!EvictBlocks(required_memory, maximum_memory)) {
		throw OutOfMemoryException("failed to pin block of size %lld%s", required_memory, InMemoryWarning());
	}
	// lock the handle again and repeat the check (in case anybody loaded in the mean time)
	lock_guard<mutex> lock(handle->lock);
	// check if the block is already loaded
	if (handle->state == BlockState::BLOCK_LOADED) {
		// the block is loaded, increment the reader count and return a pointer to the handle
		handle->readers++;
		current_memory -= required_memory;
		return handle->Load(handle);
	}
	// now we can actually load the current block
	D_ASSERT(handle->readers == 0);
	handle->readers = 1;
	return handle->Load(handle);
}

void BufferManager::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	D_ASSERT(handle->readers == 0);
	handle->eviction_timestamp++;
	PurgeQueue();
	queue->q.enqueue(make_unique<BufferEvictionNode>(weak_ptr<BlockHandle>(handle), handle->eviction_timestamp));
}

void BufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->readers > 0);
	handle->readers--;
	if (handle->readers == 0) {
		AddToEvictionQueue(handle);
	}
}

bool BufferManager::EvictBlocks(idx_t extra_memory, idx_t memory_limit) {
	PurgeQueue();

	unique_ptr<BufferEvictionNode> node;
	current_memory += extra_memory;
	while (current_memory > memory_limit) {
		// get a block to unpin from the queue
		if (!queue->q.try_dequeue(node)) {
			current_memory -= extra_memory;
			return false;
		}
		// get a reference to the underlying block pointer
		auto handle = node->TryGetBlockHandle();
		if (!handle) {
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
	return true;
}

void BufferManager::PurgeQueue() {
	unique_ptr<BufferEvictionNode> node;
	while (true) {
		if (!queue->q.try_dequeue(node)) {
			break;
		}
		auto handle = node->TryGetBlockHandle();
		if (!handle) {
			continue;
		} else {
			queue->q.enqueue(move(node));
			break;
		}
	}
}

void BufferManager::UnregisterBlock(block_id_t block_id, bool can_destroy) {
	if (block_id >= MAXIMUM_BLOCK) {
		// in-memory buffer: destroy the buffer
		if (!can_destroy) {
			// buffer could have been offloaded to disk: remove the file
			DeleteTemporaryFile(block_id);
		}
	} else {
		lock_guard<mutex> lock(blocks_lock);
		// on-disk block: erase from list of blocks in manager
		blocks.erase(block_id);
	}
}
void BufferManager::SetLimit(idx_t limit) {
	lock_guard<mutex> l_lock(limit_lock);
	// try to evict until the limit is reached
	if (!EvictBlocks(0, limit)) {
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    InMemoryWarning());
	}
	idx_t old_limit = maximum_memory;
	// set the global maximum memory to the new limit if successful
	maximum_memory = limit;
	// evict again
	if (!EvictBlocks(0, limit)) {
		// failed: go back to old limit
		maximum_memory = old_limit;
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    InMemoryWarning());
	}
}

string BufferManager::GetTemporaryPath(block_id_t id) {
	auto &fs = FileSystem::GetFileSystem(db);
	return fs.JoinPath(temp_directory, to_string(id) + ".block");
}

void BufferManager::RequireTemporaryDirectory() {
	if (temp_directory.empty()) {
		throw Exception(
		    "Out-of-memory: cannot write buffer because no temporary directory is specified!\nTo enable "
		    "temporary buffer eviction set a temporary directory using PRAGMA temp_directory='/path/to/tmp.tmp'");
	}
	lock_guard<mutex> temp_handle_guard(temp_handle_lock);
	if (!temp_directory_handle) {
		// temp directory has not been created yet: initialize it
		temp_directory_handle = make_unique<TemporaryDirectoryHandle>(db, temp_directory);
	}
}

void BufferManager::WriteTemporaryBuffer(ManagedBuffer &buffer) {
	RequireTemporaryDirectory();

	D_ASSERT(buffer.size >= Storage::BLOCK_SIZE);
	// get the path to write to
	auto path = GetTemporaryPath(buffer.id);
	// create the file and write the size followed by the buffer contents
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	handle->Write(&buffer.size, sizeof(idx_t), 0);
	buffer.Write(*handle, sizeof(idx_t));
}

unique_ptr<FileBuffer> BufferManager::ReadTemporaryBuffer(block_id_t id) {
	D_ASSERT(!temp_directory.empty());
	D_ASSERT(temp_directory_handle.get());
	idx_t block_size;
	// open the temporary file and read the size
	auto path = GetTemporaryPath(id);
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	handle->Read(&block_size, sizeof(idx_t), 0);

	// now allocate a buffer of this size and read the data into that buffer
	auto buffer = make_unique<ManagedBuffer>(db, block_size, false, id);
	buffer->Read(*handle, sizeof(idx_t));

	handle.reset();
	DeleteTemporaryFile(id);
	return move(buffer);
}

void BufferManager::DeleteTemporaryFile(block_id_t id) {
	if (temp_directory.empty() || !temp_directory_handle) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(db);
	auto path = GetTemporaryPath(id);
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}

string BufferManager::InMemoryWarning() {
	if (!temp_directory.empty()) {
		return "";
	}
	return "\nDatabase is launched in in-memory mode and no temporary directory is specified."
	       "\nUnused blocks cannot be offloaded to disk."
	       "\n\nLaunch the database with a persistent storage back-end"
	       "\nOr set PRAGMA temp_directory='/path/to/tmp.tmp'";
}

} // namespace duckdb
