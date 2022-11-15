#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

BufferPoolReservation::BufferPoolReservation(BufferPoolReservation &&src) noexcept {
	size = src.size;
	src.size = 0;
}

BufferPoolReservation &BufferPoolReservation::operator=(BufferPoolReservation &&src) noexcept {
	size = src.size;
	src.size = 0;
	return *this;
}

BufferPoolReservation::~BufferPoolReservation() {
	D_ASSERT(size == 0);
}

void BufferPoolReservation::Resize(atomic<idx_t> &counter, idx_t new_size) {
	int64_t delta = (int64_t)new_size - size;
	D_ASSERT(delta > 0 || (int64_t)counter >= -delta);
	counter += delta;
	size = new_size;
}

void BufferPoolReservation::Merge(BufferPoolReservation &&src) {
	size += src.size;
	src.size = 0;
}

struct BufferAllocatorData : PrivateAllocatorData {
	explicit BufferAllocatorData(BufferManager &manager) : manager(manager) {
	}

	BufferManager &manager;
};

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p)
    : block_manager(block_manager), readers(0), block_id(block_id_p), buffer(nullptr), eviction_timestamp(0),
      can_destroy(false), unswizzled(nullptr) {
	eviction_timestamp = 0;
	state = BlockState::BLOCK_UNLOADED;
	memory_usage = Storage::BLOCK_ALLOC_SIZE;
}

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p, unique_ptr<FileBuffer> buffer_p,
                         bool can_destroy_p, idx_t block_size, BufferPoolReservation &&reservation)
    : block_manager(block_manager), readers(0), block_id(block_id_p), eviction_timestamp(0), can_destroy(can_destroy_p),
      unswizzled(nullptr) {
	buffer = move(buffer_p);
	state = BlockState::BLOCK_LOADED;
	memory_usage = block_size;
	memory_charge = move(reservation);
}

BlockHandle::~BlockHandle() {
	// being destroyed, so any unswizzled pointers are just binary junk now.
	unswizzled = nullptr;
	auto &buffer_manager = block_manager.buffer_manager;
	// no references remain to this block: erase
	if (buffer && state == BlockState::BLOCK_LOADED) {
		D_ASSERT(memory_charge.size > 0);
		// the block is still loaded in memory: erase it
		buffer.reset();
		memory_charge.Resize(buffer_manager.current_memory, 0);
	} else {
		D_ASSERT(memory_charge.size == 0);
	}
	block_manager.UnregisterBlock(block_id, can_destroy);
}

unique_ptr<Block> AllocateBlock(BlockManager &block_manager, unique_ptr<FileBuffer> reusable_buffer,
                                block_id_t block_id) {
	if (reusable_buffer) {
		// re-usable buffer: re-use it
		if (reusable_buffer->type == FileBufferType::BLOCK) {
			// we can reuse the buffer entirely
			auto &block = (Block &)*reusable_buffer;
			block.id = block_id;
			return unique_ptr_cast<FileBuffer, Block>(move(reusable_buffer));
		}
		auto block = block_manager.CreateBlock(block_id, reusable_buffer.get());
		reusable_buffer.reset();
		return block;
	} else {
		// no re-usable buffer: allocate a new block
		return block_manager.CreateBlock(block_id, nullptr);
	}
}

unique_ptr<FileBuffer> BufferManager::ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&source,
                                                             FileBufferType type) {
	if (source) {
		auto tmp = move(source);
		D_ASSERT(tmp->size == size);
		return make_unique<FileBuffer>(*tmp, type);
	} else {
		// no re-usable buffer: allocate a new buffer
		return make_unique<FileBuffer>(Allocator::Get(db), type, size);
	}
}

BufferHandle BlockHandle::Load(shared_ptr<BlockHandle> &handle, unique_ptr<FileBuffer> reusable_buffer) {
	if (handle->state == BlockState::BLOCK_LOADED) {
		// already loaded
		D_ASSERT(handle->buffer);
		return BufferHandle(handle, handle->buffer.get());
	}

	auto &block_manager = handle->block_manager;
	if (handle->block_id < MAXIMUM_BLOCK) {
		auto block = AllocateBlock(block_manager, move(reusable_buffer), handle->block_id);
		block_manager.Read(*block);
		handle->buffer = move(block);
	} else {
		if (handle->can_destroy) {
			return BufferHandle();
		} else {
			handle->buffer = block_manager.buffer_manager.ReadTemporaryBuffer(handle->block_id, move(reusable_buffer));
		}
	}
	handle->state = BlockState::BLOCK_LOADED;
	return BufferHandle(handle, handle->buffer.get());
}

unique_ptr<FileBuffer> BlockHandle::UnloadAndTakeBlock() {
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded: nothing to do
		return nullptr;
	}
	D_ASSERT(!unswizzled);
	D_ASSERT(CanUnload());

	if (block_id >= MAXIMUM_BLOCK && !can_destroy) {
		// temporary block that cannot be destroyed: write to temporary file
		block_manager.buffer_manager.WriteTemporaryBuffer(block_id, *buffer);
	}
	memory_charge.Resize(block_manager.buffer_manager.current_memory, 0);
	state = BlockState::BLOCK_UNLOADED;
	return move(buffer);
}

void BlockHandle::Unload() {
	auto block = UnloadAndTakeBlock();
	block.reset();
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
	if (block_id >= MAXIMUM_BLOCK && !can_destroy && block_manager.buffer_manager.temp_directory.empty()) {
		// in order to unload this block we need to write it to a temporary buffer
		// however, no temporary directory is specified!
		// hence we cannot unload the block
		return false;
	}
	return true;
}

struct BufferEvictionNode {
	BufferEvictionNode() {
	}
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

typedef duckdb_moodycamel::ConcurrentQueue<BufferEvictionNode> eviction_queue_t;

struct EvictionQueue {
	eviction_queue_t q;
};

class TemporaryFileManager;

class TemporaryDirectoryHandle {
public:
	TemporaryDirectoryHandle(DatabaseInstance &db, string path_p);
	~TemporaryDirectoryHandle();

	TemporaryFileManager &GetTempFile();

private:
	DatabaseInstance &db;
	string temp_directory;
	unique_ptr<TemporaryFileManager> temp_file;
};

void BufferManager::SetTemporaryDirectory(string new_dir) {
	if (temp_directory_handle) {
		throw NotImplementedException("Cannot switch temporary directory after the current one has been used");
	}
	this->temp_directory = move(new_dir);
}

BufferManager::BufferManager(DatabaseInstance &db, string tmp, idx_t maximum_memory)
    : db(db), current_memory(0), maximum_memory(maximum_memory), temp_directory(move(tmp)),
      queue(make_unique<EvictionQueue>()), temporary_id(MAXIMUM_BLOCK),
      buffer_allocator(BufferAllocatorAllocate, BufferAllocatorFree, BufferAllocatorRealloc,
                       make_unique<BufferAllocatorData>(*this)) {
	temp_block_manager = make_unique<InMemoryBlockManager>(*this);
}

BufferManager::~BufferManager() {
}

shared_ptr<BlockHandle> BlockManager::RegisterBlock(block_id_t block_id) {
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
	auto result = make_shared<BlockHandle>(*this, block_id);
	// register the block pointer in the set of blocks as a weak pointer
	blocks[block_id] = weak_ptr<BlockHandle>(result);
	return result;
}

shared_ptr<BlockHandle> BlockManager::ConvertToPersistent(block_id_t block_id, shared_ptr<BlockHandle> old_block) {

	// pin the old block to ensure we have it loaded in memory
	auto old_handle = buffer_manager.Pin(old_block);
	D_ASSERT(old_block->state == BlockState::BLOCK_LOADED);
	D_ASSERT(old_block->buffer);

	// Temp buffers can be larger than the storage block size. But persistent buffers
	// cannot.
	D_ASSERT(old_block->buffer->AllocSize() <= Storage::BLOCK_ALLOC_SIZE);

	// register a block with the new block id
	auto new_block = RegisterBlock(block_id);
	D_ASSERT(new_block->state == BlockState::BLOCK_UNLOADED);
	D_ASSERT(new_block->readers == 0);

	// move the data from the old block into data for the new block
	new_block->state = BlockState::BLOCK_LOADED;
	new_block->buffer = CreateBlock(block_id, old_block->buffer.get());
	new_block->memory_usage = old_block->memory_usage;
	new_block->memory_charge = move(old_block->memory_charge);

	// clear the old buffer and unload it
	old_block->buffer.reset();
	old_block->state = BlockState::BLOCK_UNLOADED;
	old_block->memory_usage = 0;
	old_handle.Destroy();
	old_block.reset();

	// persist the new block to disk
	Write(*new_block->buffer, block_id);

	buffer_manager.AddToEvictionQueue(new_block);

	return new_block;
}

template <typename... ARGS>
TempBufferPoolReservation BufferManager::EvictBlocksOrThrow(idx_t memory_delta, idx_t limit,
                                                            unique_ptr<FileBuffer> *buffer, ARGS... args) {
	auto r = EvictBlocks(memory_delta, limit, buffer);
	if (!r.success) {
		throw OutOfMemoryException(args..., InMemoryWarning());
	}
	return move(r.reservation);
}

shared_ptr<BlockHandle> BufferManager::RegisterSmallMemory(idx_t block_size) {
	auto res = EvictBlocksOrThrow(block_size, maximum_memory, nullptr,
	                              "could not allocate block of %lld bytes (%lld/%lld used) %s", block_size,
	                              GetUsedMemory(), GetMaxMemory());

	auto buffer = ConstructManagedBuffer(block_size, nullptr, FileBufferType::TINY_BUFFER);

	// create a new block pointer for this block
	return make_shared<BlockHandle>(*temp_block_manager, ++temporary_id, move(buffer), false, block_size, move(res));
}

shared_ptr<BlockHandle> BufferManager::RegisterMemory(idx_t block_size, bool can_destroy) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	auto alloc_size = AlignValue<idx_t, Storage::SECTOR_SIZE>(block_size + Storage::BLOCK_HEADER_SIZE);
	// first evict blocks until we have enough memory to store this buffer
	unique_ptr<FileBuffer> reusable_buffer;
	auto res = EvictBlocksOrThrow(alloc_size, maximum_memory, &reusable_buffer,
	                              "could not allocate block of %lld bytes (%lld/%lld used) %s", alloc_size,
	                              GetUsedMemory(), GetMaxMemory());

	auto buffer = ConstructManagedBuffer(block_size, move(reusable_buffer));

	// create a new block pointer for this block
	return make_shared<BlockHandle>(*temp_block_manager, ++temporary_id, move(buffer), can_destroy, alloc_size,
	                                move(res));
}

BufferHandle BufferManager::Allocate(idx_t block_size) {
	auto block = RegisterMemory(block_size, true);
	return Pin(block);
}

void BufferManager::ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->state == BlockState::BLOCK_LOADED);
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
	D_ASSERT(handle->memory_usage == handle->memory_charge.size);

	auto req = handle->buffer->CalculateMemory(block_size);
	int64_t memory_delta = (int64_t)req.alloc_size - handle->memory_usage;

	if (memory_delta == 0) {
		return;
	} else if (memory_delta > 0) {
		// evict blocks until we have space to resize this block
		auto reservation =
		    EvictBlocksOrThrow(memory_delta, maximum_memory, nullptr, "failed to resize block from %lld to %lld%s",
		                       handle->memory_usage, req.alloc_size);
		// EvictBlocks decrements 'current_memory' for us.
		handle->memory_charge.Merge(move(reservation));
	} else {
		// no need to evict blocks, but we do need to decrement 'current_memory'.
		handle->memory_charge.Resize(current_memory, req.alloc_size);
	}

	// resize and adjust current memory
	handle->buffer->Resize(block_size);
	handle->memory_usage += memory_delta;
}

BufferHandle BufferManager::Pin(shared_ptr<BlockHandle> &handle) {
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
	unique_ptr<FileBuffer> reusable_buffer;
	auto reservation = EvictBlocksOrThrow(required_memory, maximum_memory, &reusable_buffer,
	                                      "failed to pin block of size %lld%s", required_memory);
	// lock the handle again and repeat the check (in case anybody loaded in the mean time)
	lock_guard<mutex> lock(handle->lock);
	// check if the block is already loaded
	if (handle->state == BlockState::BLOCK_LOADED) {
		// the block is loaded, increment the reader count and return a pointer to the handle
		handle->readers++;
		reservation.Resize(current_memory, 0);
		return handle->Load(handle);
	}
	// now we can actually load the current block
	D_ASSERT(handle->readers == 0);
	handle->readers = 1;
	auto buf = handle->Load(handle, move(reusable_buffer));
	handle->memory_charge = move(reservation);
	// In the case of a variable sized block, the buffer may be smaller than a full block.
	int64_t delta = handle->buffer->AllocSize() - handle->memory_usage;
	if (delta) {
		D_ASSERT(delta < 0);
		handle->memory_usage += delta;
		handle->memory_charge.Resize(current_memory, handle->memory_usage);
	}
	return buf;
}

void BufferManager::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	constexpr int INSERT_INTERVAL = 1024;

	D_ASSERT(handle->readers == 0);
	handle->eviction_timestamp++;
	// After each 1024 insertions, run through the queue and purge.
	if ((++queue_insertions % INSERT_INTERVAL) == 0) {
		PurgeQueue();
	}
	queue->q.enqueue(BufferEvictionNode(weak_ptr<BlockHandle>(handle), handle->eviction_timestamp));
}

void BufferManager::VerifyZeroReaders(shared_ptr<BlockHandle> &handle) {
#ifdef DUCKDB_DEBUG_DESTROY_BLOCKS
	auto replacement_buffer = make_unique<FileBuffer>(Allocator::Get(db), handle->buffer->type,
	                                                  handle->memory_usage - Storage::BLOCK_HEADER_SIZE);
	memcpy(replacement_buffer->buffer, handle->buffer->buffer, handle->buffer->size);
	memset(handle->buffer->buffer, 190, handle->buffer->size);
	handle->buffer = move(replacement_buffer);
#endif
}

void BufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
	lock_guard<mutex> lock(handle->lock);
	if (!handle->buffer || handle->buffer->type == FileBufferType::TINY_BUFFER) {
		return;
	}
	D_ASSERT(handle->readers > 0);
	handle->readers--;
	if (handle->readers == 0) {
		VerifyZeroReaders(handle);
		AddToEvictionQueue(handle);
	}
}

BufferManager::EvictionResult BufferManager::EvictBlocks(idx_t extra_memory, idx_t memory_limit,
                                                         unique_ptr<FileBuffer> *buffer) {
	BufferEvictionNode node;
	TempBufferPoolReservation r(current_memory, extra_memory);
	while (current_memory > memory_limit) {
		// get a block to unpin from the queue
		if (!queue->q.try_dequeue(node)) {
			// Failed to reserve. Adjust size of temp reservation to 0.
			r.Resize(current_memory, 0);
			return {false, move(r)};
		}
		// get a reference to the underlying block pointer
		auto handle = node.TryGetBlockHandle();
		if (!handle) {
			continue;
		}
		// we might be able to free this block: grab the mutex and check if we can free it
		lock_guard<mutex> lock(handle->lock);
		if (!node.CanUnload(*handle)) {
			// something changed in the mean-time, bail out
			continue;
		}
		// hooray, we can unload the block
		if (buffer && handle->buffer->AllocSize() == extra_memory) {
			// we can actually re-use the memory directly!
			*buffer = handle->UnloadAndTakeBlock();
			return {true, move(r)};
		} else {
			// release the memory and mark the block as unloaded
			handle->Unload();
		}
	}
	return {true, move(r)};
}

void BufferManager::PurgeQueue() {
	BufferEvictionNode node;
	while (true) {
		if (!queue->q.try_dequeue(node)) {
			break;
		}
		auto handle = node.TryGetBlockHandle();
		if (!handle) {
			continue;
		} else {
			queue->q.enqueue(move(node));
			break;
		}
	}
}

void BlockManager::UnregisterBlock(block_id_t block_id, bool can_destroy) {
	if (block_id >= MAXIMUM_BLOCK) {
		// in-memory buffer: destroy the buffer
		if (!can_destroy) {
			// buffer could have been offloaded to disk: remove the file
			buffer_manager.DeleteTemporaryFile(block_id);
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
	if (!EvictBlocks(0, limit).success) {
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    InMemoryWarning());
	}
	idx_t old_limit = maximum_memory;
	// set the global maximum memory to the new limit if successful
	maximum_memory = limit;
	// evict again
	if (!EvictBlocks(0, limit).success) {
		// failed: go back to old limit
		maximum_memory = old_limit;
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    InMemoryWarning());
	}
}

//===--------------------------------------------------------------------===//
// Temporary File Management
//===--------------------------------------------------------------------===//
unique_ptr<FileBuffer> ReadTemporaryBufferInternal(BufferManager &buffer_manager, FileHandle &handle, idx_t position,
                                                   idx_t size, block_id_t id, unique_ptr<FileBuffer> reusable_buffer) {
	auto buffer = buffer_manager.ConstructManagedBuffer(size, move(reusable_buffer));
	buffer->Read(handle, position);
	return buffer;
}

struct TemporaryFileIndex {
	explicit TemporaryFileIndex(idx_t file_index = DConstants::INVALID_INDEX,
	                            idx_t block_index = DConstants::INVALID_INDEX)
	    : file_index(file_index), block_index(block_index) {
	}

	idx_t file_index;
	idx_t block_index;

public:
	bool IsValid() {
		return block_index != DConstants::INVALID_INDEX;
	}
};

struct BlockIndexManager {
	BlockIndexManager() : max_index(0) {
	}

public:
	//! Obtains a new block index from the index manager
	idx_t GetNewBlockIndex() {
		auto index = GetNewBlockIndexInternal();
		indexes_in_use.insert(index);
		return index;
	}

	//! Removes an index from the block manager
	//! Returns true if the max_index has been altered
	bool RemoveIndex(idx_t index) {
		// remove this block from the set of blocks
		indexes_in_use.erase(index);
		free_indexes.insert(index);
		// check if we can truncate the file

		// get the max_index in use right now
		auto max_index_in_use = indexes_in_use.empty() ? 0 : *indexes_in_use.rbegin();
		if (max_index_in_use < max_index) {
			// max index in use is lower than the max_index
			// reduce the max_index
			max_index = max_index_in_use + 1;
			// we can remove any free_indexes that are larger than the current max_index
			while (!free_indexes.empty()) {
				auto max_entry = *free_indexes.rbegin();
				if (max_entry < max_index) {
					break;
				}
				free_indexes.erase(max_entry);
			}
			return true;
		}
		return false;
	}

	idx_t GetMaxIndex() {
		return max_index;
	}

	bool HasFreeBlocks() {
		return !free_indexes.empty();
	}

private:
	idx_t GetNewBlockIndexInternal() {
		if (free_indexes.empty()) {
			return max_index++;
		}
		auto entry = free_indexes.begin();
		auto index = *entry;
		free_indexes.erase(entry);
		return index;
	}

	idx_t max_index;
	set<idx_t> free_indexes;
	set<idx_t> indexes_in_use;
};

class TemporaryFileHandle {
	constexpr static idx_t MAX_ALLOWED_INDEX = 4000;

public:
	TemporaryFileHandle(DatabaseInstance &db, const string &temp_directory, idx_t index)
	    : db(db), file_index(index), path(FileSystem::GetFileSystem(db).JoinPath(
	                                     temp_directory, "duckdb_temp_storage-" + to_string(index) + ".tmp")) {
	}

public:
	struct TemporaryFileLock {
		explicit TemporaryFileLock(mutex &mutex) : lock(mutex) {
		}

		lock_guard<mutex> lock;
	};

public:
	TemporaryFileIndex TryGetBlockIndex() {
		TemporaryFileLock lock(file_lock);
		if (index_manager.GetMaxIndex() >= MAX_ALLOWED_INDEX && index_manager.HasFreeBlocks()) {
			// file is at capacity
			return TemporaryFileIndex();
		}
		// open the file handle if it does not yet exist
		CreateFileIfNotExists(lock);
		// fetch a new block index to write to
		auto block_index = index_manager.GetNewBlockIndex();
		return TemporaryFileIndex(file_index, block_index);
	}

	void WriteTemporaryFile(FileBuffer &buffer, TemporaryFileIndex index) {
		D_ASSERT(buffer.size == Storage::BLOCK_SIZE);
		buffer.Write(*handle, GetPositionInFile(index.block_index));
	}

	unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, idx_t block_index,
	                                           unique_ptr<FileBuffer> reusable_buffer) {
		auto buffer =
		    ReadTemporaryBufferInternal(BufferManager::GetBufferManager(db), *handle, GetPositionInFile(block_index),
		                                Storage::BLOCK_SIZE, id, move(reusable_buffer));
		{
			// remove the block (and potentially truncate the temp file)
			TemporaryFileLock lock(file_lock);
			D_ASSERT(handle);
			RemoveTempBlockIndex(lock, block_index);
		}
		return buffer;
	}

	bool DeleteIfEmpty() {
		TemporaryFileLock lock(file_lock);
		if (index_manager.GetMaxIndex() > 0) {
			// there are still blocks in this file
			return false;
		}
		// the file is empty: delete it
		handle.reset();
		auto &fs = FileSystem::GetFileSystem(db);
		fs.RemoveFile(path);
		return true;
	}

private:
	void CreateFileIfNotExists(TemporaryFileLock &) {
		if (handle) {
			return;
		}
		auto &fs = FileSystem::GetFileSystem(db);
		handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE |
		                               FileFlags::FILE_FLAGS_FILE_CREATE);
	}

	void RemoveTempBlockIndex(TemporaryFileLock &, idx_t index) {
		// remove the block index from the index manager
		if (index_manager.RemoveIndex(index)) {
			// the max_index that is currently in use has decreased
			// as a result we can truncate the file
			auto max_index = index_manager.GetMaxIndex();
			auto &fs = FileSystem::GetFileSystem(db);
			fs.Truncate(*handle, GetPositionInFile(max_index + 1));
		}
	}

	idx_t GetPositionInFile(idx_t index) {
		return index * Storage::BLOCK_ALLOC_SIZE;
	}

private:
	DatabaseInstance &db;
	unique_ptr<FileHandle> handle;
	idx_t file_index;
	string path;
	mutex file_lock;
	BlockIndexManager index_manager;
};

class TemporaryFileManager {
public:
	TemporaryFileManager(DatabaseInstance &db, const string &temp_directory_p)
	    : db(db), temp_directory(temp_directory_p) {
	}

public:
	struct TemporaryManagerLock {
		explicit TemporaryManagerLock(mutex &mutex) : lock(mutex) {
		}

		lock_guard<mutex> lock;
	};

	void WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer) {
		D_ASSERT(buffer.size == Storage::BLOCK_SIZE);
		TemporaryFileIndex index;
		TemporaryFileHandle *handle = nullptr;

		{
			TemporaryManagerLock lock(manager_lock);
			// first check if we can write to an open existing file
			for (auto &entry : files) {
				auto &temp_file = entry.second;
				index = temp_file->TryGetBlockIndex();
				if (index.IsValid()) {
					handle = entry.second.get();
					break;
				}
			}
			if (!handle) {
				// no existing handle to write to; we need to create & open a new file
				auto new_file_index = index_manager.GetNewBlockIndex();
				auto new_file = make_unique<TemporaryFileHandle>(db, temp_directory, new_file_index);
				handle = new_file.get();
				files[new_file_index] = move(new_file);

				index = handle->TryGetBlockIndex();
			}
			D_ASSERT(used_blocks.find(block_id) == used_blocks.end());
			used_blocks[block_id] = index;
		}
		D_ASSERT(handle);
		D_ASSERT(index.IsValid());
		handle->WriteTemporaryFile(buffer, index);
	}

	bool HasTemporaryBuffer(block_id_t block_id) {
		lock_guard<mutex> lock(manager_lock);
		return used_blocks.find(block_id) != used_blocks.end();
	}

	unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> reusable_buffer) {
		TemporaryFileIndex index;
		TemporaryFileHandle *handle;
		{
			TemporaryManagerLock lock(manager_lock);
			index = GetTempBlockIndex(lock, id);
			handle = GetFileHandle(lock, index.file_index);
		}
		auto buffer = handle->ReadTemporaryBuffer(id, index.block_index, move(reusable_buffer));
		{
			// remove the block (and potentially erase the temp file)
			TemporaryManagerLock lock(manager_lock);
			EraseUsedBlock(lock, id, handle, index.file_index);
		}
		return buffer;
	}

	void DeleteTemporaryBuffer(block_id_t id) {
		TemporaryManagerLock lock(manager_lock);
		auto index = GetTempBlockIndex(lock, id);
		auto handle = GetFileHandle(lock, index.file_index);
		EraseUsedBlock(lock, id, handle, index.file_index);
	}

private:
	void EraseUsedBlock(TemporaryManagerLock &lock, block_id_t id, TemporaryFileHandle *handle, idx_t file_index) {
		used_blocks.erase(id);
		if (handle->DeleteIfEmpty()) {
			EraseFileHandle(lock, file_index);
		}
	}

	TemporaryFileHandle *GetFileHandle(TemporaryManagerLock &, idx_t index) {
		return files[index].get();
	}

	TemporaryFileIndex GetTempBlockIndex(TemporaryManagerLock &, block_id_t id) {
		D_ASSERT(used_blocks.find(id) != used_blocks.end());
		return used_blocks[id];
	}

	void EraseFileHandle(TemporaryManagerLock &, idx_t file_index) {
		files.erase(file_index);
		index_manager.RemoveIndex(file_index);
	}

private:
	DatabaseInstance &db;
	mutex manager_lock;
	//! The temporary directory
	string temp_directory;
	//! The set of active temporary file handles
	unordered_map<idx_t, unique_ptr<TemporaryFileHandle>> files;
	//! map of block_id -> temporary file position
	unordered_map<block_id_t, TemporaryFileIndex> used_blocks;
	//! Manager of in-use temporary file indexes
	BlockIndexManager index_manager;
};

TemporaryDirectoryHandle::TemporaryDirectoryHandle(DatabaseInstance &db, string path_p)
    : db(db), temp_directory(move(path_p)), temp_file(make_unique<TemporaryFileManager>(db, temp_directory)) {
	auto &fs = FileSystem::GetFileSystem(db);
	if (!temp_directory.empty()) {
		fs.CreateDirectory(temp_directory);
	}
}
TemporaryDirectoryHandle::~TemporaryDirectoryHandle() {
	// first release any temporary files
	temp_file.reset();
	// then delete the temporary file directory
	auto &fs = FileSystem::GetFileSystem(db);
	if (!temp_directory.empty()) {
		fs.RemoveDirectory(temp_directory);
	}
}

TemporaryFileManager &TemporaryDirectoryHandle::GetTempFile() {
	return *temp_file;
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

void BufferManager::WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer) {
	RequireTemporaryDirectory();
	if (buffer.size == Storage::BLOCK_SIZE) {
		temp_directory_handle->GetTempFile().WriteTemporaryBuffer(block_id, buffer);
		return;
	}
	// get the path to write to
	auto path = GetTemporaryPath(block_id);
	D_ASSERT(buffer.size > Storage::BLOCK_SIZE);
	// create the file and write the size followed by the buffer contents
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	handle->Write(&buffer.size, sizeof(idx_t), 0);
	buffer.Write(*handle, sizeof(idx_t));
}

unique_ptr<FileBuffer> BufferManager::ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> reusable_buffer) {
	D_ASSERT(!temp_directory.empty());
	D_ASSERT(temp_directory_handle.get());
	if (temp_directory_handle->GetTempFile().HasTemporaryBuffer(id)) {
		return temp_directory_handle->GetTempFile().ReadTemporaryBuffer(id, move(reusable_buffer));
	}
	idx_t block_size;
	// open the temporary file and read the size
	auto path = GetTemporaryPath(id);
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	handle->Read(&block_size, sizeof(idx_t), 0);

	// now allocate a buffer of this size and read the data into that buffer
	auto buffer = ReadTemporaryBufferInternal(*this, *handle, sizeof(idx_t), block_size, id, move(reusable_buffer));

	handle.reset();
	DeleteTemporaryFile(id);
	return buffer;
}

void BufferManager::DeleteTemporaryFile(block_id_t id) {
	if (temp_directory.empty()) {
		// no temporary directory specified: nothing to delete
		return;
	}
	{
		lock_guard<mutex> temp_handle_guard(temp_handle_lock);
		if (!temp_directory_handle) {
			// temporary directory was not initialized yet: nothing to delete
			return;
		}
	}
	// check if we should delete the file from the shared pool of files, or from the general file system
	if (temp_directory_handle->GetTempFile().HasTemporaryBuffer(id)) {
		temp_directory_handle->GetTempFile().DeleteTemporaryBuffer(id);
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

void BufferManager::ReserveMemory(idx_t size) {
	if (size == 0) {
		return;
	}
	auto reservation =
	    EvictBlocksOrThrow(size, maximum_memory, nullptr, "failed to reserve memory data of size %lld%s", size);
	reservation.size = 0;
}

void BufferManager::FreeReservedMemory(idx_t size) {
	if (size == 0) {
		return;
	}
	current_memory -= size;
}

//===--------------------------------------------------------------------===//
// Buffer Allocator
//===--------------------------------------------------------------------===//
data_ptr_t BufferManager::BufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
	auto &data = (BufferAllocatorData &)*private_data;
	auto reservation = data.manager.EvictBlocksOrThrow(size, data.manager.maximum_memory, nullptr,
	                                                   "failed to allocate data of size %lld%s", size);
	// We rely on manual tracking of this one. :(
	reservation.size = 0;
	return Allocator::Get(data.manager.db).AllocateData(size);
}

void BufferManager::BufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	auto &data = (BufferAllocatorData &)*private_data;
	BufferPoolReservation r;
	r.size = size;
	r.Resize(data.manager.current_memory, 0);
	return Allocator::Get(data.manager.db).FreeData(pointer, size);
}

data_ptr_t BufferManager::BufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                                 idx_t size) {
	auto &data = (BufferAllocatorData &)*private_data;
	BufferPoolReservation r;
	r.size = old_size;
	r.Resize(data.manager.current_memory, size);
	r.size = 0;
	return Allocator::Get(data.manager.db).ReallocateData(pointer, old_size, size);
}

Allocator &BufferAllocator::Get(ClientContext &context) {
	auto &manager = BufferManager::GetBufferManager(context);
	return manager.GetBufferAllocator();
}

Allocator &BufferManager::GetBufferAllocator() {
	return buffer_allocator;
}

} // namespace duckdb
