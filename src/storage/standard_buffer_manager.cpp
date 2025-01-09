#include "duckdb/storage/standard_buffer_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

namespace duckdb {

#ifdef DUCKDB_DEBUG_DESTROY_BLOCKS
static void WriteGarbageIntoBuffer(BlockLock &lock, BlockHandle &block) {
	auto &buffer = block.GetBuffer(lock);
	memset(buffer->buffer, 0xa5, buffer->size); // 0xa5 is default memory in debug mode
}

static void WriteGarbageIntoBuffer(BlockHandle &block) {
	auto lock = block.GetLock();
	WriteGarbageIntoBuffer(lock, block);
}
#endif

struct BufferAllocatorData : PrivateAllocatorData {
	explicit BufferAllocatorData(StandardBufferManager &manager) : manager(manager) {
	}

	StandardBufferManager &manager;
};

unique_ptr<FileBuffer> StandardBufferManager::ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&source,
                                                                     FileBufferType type) {
	unique_ptr<FileBuffer> result;
	if (type == FileBufferType::BLOCK) {
		throw InternalException("ConstructManagedBuffer cannot be used to construct blocks");
	}
	if (source) {
		auto tmp = std::move(source);
		D_ASSERT(tmp->AllocSize() == BufferManager::GetAllocSize(size));
		result = make_uniq<FileBuffer>(*tmp, type);
	} else {
		// no re-usable buffer: allocate a new buffer
		result = make_uniq<FileBuffer>(Allocator::Get(db), type, size);
	}
	result->Initialize(DBConfig::GetConfig(db).options.debug_initialize);
	return result;
}

void StandardBufferManager::SetTemporaryDirectory(const string &new_dir) {
	lock_guard<mutex> guard(temporary_directory.lock);
	if (temporary_directory.handle) {
		throw NotImplementedException("Cannot switch temporary directory after the current one has been used");
	}
	temporary_directory.path = new_dir;
}

StandardBufferManager::StandardBufferManager(DatabaseInstance &db, string tmp)
    : BufferManager(), db(db), buffer_pool(db.GetBufferPool()), temporary_id(MAXIMUM_BLOCK),
      buffer_allocator(BufferAllocatorAllocate, BufferAllocatorFree, BufferAllocatorRealloc,
                       make_uniq<BufferAllocatorData>(*this)) {
	temp_block_manager = make_uniq<InMemoryBlockManager>(*this, DEFAULT_BLOCK_ALLOC_SIZE);
	temporary_directory.path = std::move(tmp);
	for (idx_t i = 0; i < MEMORY_TAG_COUNT; i++) {
		evicted_data_per_tag[i] = 0;
	}
}

StandardBufferManager::~StandardBufferManager() {
}

BufferPool &StandardBufferManager::GetBufferPool() const {
	return buffer_pool;
}

TemporaryMemoryManager &StandardBufferManager::GetTemporaryMemoryManager() {
	return buffer_pool.GetTemporaryMemoryManager();
}

idx_t StandardBufferManager::GetUsedMemory() const {
	return buffer_pool.GetUsedMemory();
}
idx_t StandardBufferManager::GetMaxMemory() const {
	return buffer_pool.GetMaxMemory();
}

idx_t StandardBufferManager::GetUsedSwap() {
	lock_guard<mutex> guard(temporary_directory.lock);
	if (!temporary_directory.handle) {
		return 0;
	}
	return temporary_directory.handle->GetTempFile().GetTotalUsedSpaceInBytes();
}

optional_idx StandardBufferManager::GetMaxSwap() const {
	lock_guard<mutex> guard(temporary_directory.lock);
	if (!temporary_directory.handle) {
		return optional_idx();
	}
	return temporary_directory.handle->GetTempFile().GetMaxSwapSpace();
}

idx_t StandardBufferManager::GetBlockAllocSize() const {
	return temp_block_manager->GetBlockAllocSize();
}

idx_t StandardBufferManager::GetBlockSize() const {
	return temp_block_manager->GetBlockSize();
}

template <typename... ARGS>
TempBufferPoolReservation StandardBufferManager::EvictBlocksOrThrow(MemoryTag tag, idx_t memory_delta,
                                                                    unique_ptr<FileBuffer> *buffer, ARGS... args) {
	auto r = buffer_pool.EvictBlocks(tag, memory_delta, buffer_pool.maximum_memory, buffer);
	if (!r.success) {
		string extra_text = StringUtil::Format(" (%s/%s used)", StringUtil::BytesToHumanReadableString(GetUsedMemory()),
		                                       StringUtil::BytesToHumanReadableString(GetMaxMemory()));
		extra_text += InMemoryWarning();
		throw OutOfMemoryException(args..., extra_text);
	}
	return std::move(r.reservation);
}

shared_ptr<BlockHandle> StandardBufferManager::RegisterTransientMemory(const idx_t size, const idx_t block_size) {
	D_ASSERT(size <= block_size);

	// This comparison is the reason behind passing block_size through transient memory creation.
	// Otherwise, any non-default block size would register as small memory, causing problems when
	// trying to convert that memory to consistent blocks later on.
	if (size < block_size) {
		return RegisterSmallMemory(MemoryTag::IN_MEMORY_TABLE, size);
	}

	auto buffer_handle = Allocate(MemoryTag::IN_MEMORY_TABLE, size, false);
	return buffer_handle.GetBlockHandle();
}

shared_ptr<BlockHandle> StandardBufferManager::RegisterSmallMemory(MemoryTag tag, const idx_t size) {
	D_ASSERT(size < GetBlockSize());
	auto reservation = EvictBlocksOrThrow(tag, size, nullptr, "could not allocate block of size %s%s",
	                                      StringUtil::BytesToHumanReadableString(size));

	auto buffer = ConstructManagedBuffer(size, nullptr, FileBufferType::TINY_BUFFER);

	// Create a new block pointer for this block.
	auto result = make_shared_ptr<BlockHandle>(*temp_block_manager, ++temporary_id, tag, std::move(buffer),
	                                           DestroyBufferUpon::BLOCK, size, std::move(reservation));
#ifdef DUCKDB_DEBUG_DESTROY_BLOCKS
	// Initialize the memory with garbage data
	WriteGarbageIntoBuffer(*result);
#endif
	return result;
}

shared_ptr<BlockHandle> StandardBufferManager::RegisterMemory(MemoryTag tag, idx_t block_size, bool can_destroy) {
	auto alloc_size = GetAllocSize(block_size);

	// Evict blocks until there is enough memory to store the buffer.
	unique_ptr<FileBuffer> reusable_buffer;
	auto res = EvictBlocksOrThrow(tag, alloc_size, &reusable_buffer, "could not allocate block of size %s%s",
	                              StringUtil::BytesToHumanReadableString(alloc_size));

	// Create a new buffer and a block to hold the buffer.
	auto buffer = ConstructManagedBuffer(block_size, std::move(reusable_buffer));
	DestroyBufferUpon destroy_buffer_upon = can_destroy ? DestroyBufferUpon::EVICTION : DestroyBufferUpon::BLOCK;
	return make_shared_ptr<BlockHandle>(*temp_block_manager, ++temporary_id, tag, std::move(buffer),
	                                    destroy_buffer_upon, alloc_size, std::move(res));
}

BufferHandle StandardBufferManager::Allocate(MemoryTag tag, idx_t block_size, bool can_destroy) {
	auto block = RegisterMemory(tag, block_size, can_destroy);

#ifdef DUCKDB_DEBUG_DESTROY_BLOCKS
	// Initialize the memory with garbage data
	WriteGarbageIntoBuffer(*block);
#endif
	return Pin(block);
}

void StandardBufferManager::ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) {
	D_ASSERT(block_size >= GetBlockSize());
	auto lock = handle->GetLock();

	auto handle_memory_usage = handle->GetMemoryUsage();
	D_ASSERT(handle->GetState() == BlockState::BLOCK_LOADED);
	D_ASSERT(handle_memory_usage == handle->GetBuffer(lock)->AllocSize());
	D_ASSERT(handle_memory_usage == handle->GetMemoryCharge(lock).size);

	auto req = handle->GetBuffer(lock)->CalculateMemory(block_size);
	int64_t memory_delta = NumericCast<int64_t>(req.alloc_size) - NumericCast<int64_t>(handle_memory_usage);

	if (memory_delta == 0) {
		return;
	} else if (memory_delta > 0) {
		// evict blocks until we have space to resize this block
		// unlock the handle lock during the call to EvictBlocksOrThrow
		lock.unlock();
		auto reservation = EvictBlocksOrThrow(handle->GetMemoryTag(), NumericCast<idx_t>(memory_delta), nullptr,
		                                      "failed to resize block from %s to %s%s",
		                                      StringUtil::BytesToHumanReadableString(handle_memory_usage),
		                                      StringUtil::BytesToHumanReadableString(req.alloc_size));
		lock.lock();

		// EvictBlocks decrements 'current_memory' for us.
		handle->MergeMemoryReservation(lock, std::move(reservation));
	} else {
		// no need to evict blocks, but we do need to decrement 'current_memory'.
		handle->ResizeMemory(lock, req.alloc_size);
	}

	handle->ResizeBuffer(lock, block_size, memory_delta);
}

void StandardBufferManager::BatchRead(vector<shared_ptr<BlockHandle>> &handles, const map<block_id_t, idx_t> &load_map,
                                      block_id_t first_block, block_id_t last_block) {
	auto &block_manager = handles[0]->block_manager;
	idx_t block_count = NumericCast<idx_t>(last_block - first_block + 1);
#ifndef DUCKDB_ALTERNATIVE_VERIFY
	if (block_count == 1) {
		// prefetching with block_count == 1 has no performance impact since we can't batch reads
		// skip the prefetch in this case
		// we do it anyway if alternative_verify is on for extra testing
		return;
	}
#endif

	// allocate a buffer to hold the data of all of the blocks
	auto intermediate_buffer = Allocate(MemoryTag::BASE_TABLE, block_count * block_manager.GetBlockSize());
	// perform a batch read of the blocks into the buffer
	block_manager.ReadBlocks(intermediate_buffer.GetFileBuffer(), first_block, block_count);

	// the blocks are read - now we need to assign them to the individual blocks
	for (idx_t block_idx = 0; block_idx < block_count; block_idx++) {
		block_id_t block_id = first_block + NumericCast<block_id_t>(block_idx);
		auto entry = load_map.find(block_id);
		D_ASSERT(entry != load_map.end()); // if we allow gaps we might not return true here
		auto &handle = handles[entry->second];

		// reserve memory for the block
		idx_t required_memory = handle->GetMemoryUsage();
		unique_ptr<FileBuffer> reusable_buffer;
		auto reservation = EvictBlocksOrThrow(handle->GetMemoryTag(), required_memory, &reusable_buffer,
		                                      "failed to pin block of size %s%s",
		                                      StringUtil::BytesToHumanReadableString(required_memory));
		// now load the block from the buffer
		// note that we discard the buffer handle - we do not keep it around
		// the prefetching relies on the block handle being pinned again during the actual read before it is evicted
		BufferHandle buf;
		{
			auto lock = handle->GetLock();
			if (handle->GetState() == BlockState::BLOCK_LOADED) {
				// the block is loaded already by another thread - free up the reservation and continue
				reservation.Resize(0);
				continue;
			}
			auto block_ptr =
			    intermediate_buffer.GetFileBuffer().InternalBuffer() + block_idx * block_manager.GetBlockAllocSize();
			buf = handle->LoadFromBuffer(lock, block_ptr, std::move(reusable_buffer), std::move(reservation));
		}
	}
}

void StandardBufferManager::Prefetch(vector<shared_ptr<BlockHandle>> &handles) {
	// figure out which set of blocks we should load
	map<block_id_t, idx_t> to_be_loaded;
	for (idx_t block_idx = 0; block_idx < handles.size(); block_idx++) {
		auto &handle = handles[block_idx];
		if (handle->GetState() != BlockState::BLOCK_LOADED) {
			// need to load this block - add it to the map
			to_be_loaded.insert(make_pair(handle->BlockId(), block_idx));
		}
	}
	if (to_be_loaded.empty()) {
		// nothing to fetch
		return;
	}
	// iterate over the blocks and perform bulk reads
	block_id_t first_block = -1;
	block_id_t previous_block_id = -1;
	for (auto &entry : to_be_loaded) {
		if (previous_block_id < 0) {
			// this the first block we are seeing
			first_block = entry.first;
			previous_block_id = first_block;
		} else if (previous_block_id + 1 == entry.first) {
			// this block is adjacent to the previous block - add it to the batch read
			previous_block_id = entry.first;
		} else {
			// this block is not adjacent to the previous block
			// perform the batch read for the previous batch
			BatchRead(handles, to_be_loaded, first_block, previous_block_id);

			// set the first_block and previous_block_id to the current block
			first_block = entry.first;
			previous_block_id = entry.first;
		}
	}
	// batch read the final batch
	BatchRead(handles, to_be_loaded, first_block, previous_block_id);
}

BufferHandle StandardBufferManager::Pin(shared_ptr<BlockHandle> &handle) {
	// we need to be careful not to return the BufferHandle to this block while holding the BlockHandle's lock
	// as exiting this function's scope may cause the destructor of the BufferHandle to be called while holding the lock
	// the destructor calls Unpin, which grabs the BlockHandle's lock again, causing a deadlock
	BufferHandle buf;

	idx_t required_memory;
	{
		// lock the block
		auto lock = handle->GetLock();
		// check if the block is already loaded
		if (handle->GetState() == BlockState::BLOCK_LOADED) {
			// the block is loaded, increment the reader count and set the BufferHandle
			buf = handle->Load();
		}
		required_memory = handle->GetMemoryUsage();
	}

	if (buf.IsValid()) {
		return buf; // the block was already loaded, return it without holding the BlockHandle's lock
	} else {
		// evict blocks until we have space for the current block
		unique_ptr<FileBuffer> reusable_buffer;
		auto reservation = EvictBlocksOrThrow(handle->GetMemoryTag(), required_memory, &reusable_buffer,
		                                      "failed to pin block of size %s%s",
		                                      StringUtil::BytesToHumanReadableString(required_memory));

		// lock the handle again and repeat the check (in case anybody loaded in the meantime)
		auto lock = handle->GetLock();
		// check if the block is already loaded
		if (handle->GetState() == BlockState::BLOCK_LOADED) {
			// the block is loaded, increment the reader count and return a pointer to the handle
			reservation.Resize(0);
			buf = handle->Load();
		} else {
			// now we can actually load the current block
			D_ASSERT(handle->Readers() == 0);
			buf = handle->Load(std::move(reusable_buffer));
			auto &memory_charge = handle->GetMemoryCharge(lock);
			memory_charge = std::move(reservation);
			// in the case of a variable sized block, the buffer may be smaller than a full block.
			int64_t delta = NumericCast<int64_t>(handle->GetBuffer(lock)->AllocSize()) -
			                NumericCast<int64_t>(handle->GetMemoryUsage());
			if (delta) {
				handle->ChangeMemoryUsage(lock, delta);
			}
			D_ASSERT(handle->GetMemoryUsage() == handle->GetBuffer(lock)->AllocSize());
		}
	}

	// we should have a valid BufferHandle by now, either because the block was already loaded, or because we loaded it
	// return it without holding the BlockHandle's lock
	D_ASSERT(buf.IsValid());
	return buf;
}

void StandardBufferManager::PurgeQueue(const BlockHandle &handle) {
	buffer_pool.PurgeQueue(handle);
}

void StandardBufferManager::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	buffer_pool.AddToEvictionQueue(handle);
}

void StandardBufferManager::VerifyZeroReaders(BlockLock &lock, shared_ptr<BlockHandle> &handle) {
#ifdef DUCKDB_DEBUG_DESTROY_BLOCKS
	unique_ptr<FileBuffer> replacement_buffer;
	auto &allocator = Allocator::Get(db);
	auto alloc_size = handle->GetMemoryUsage() - Storage::DEFAULT_BLOCK_HEADER_SIZE;
	auto &buffer = handle->GetBuffer(lock);
	if (handle->GetBufferType() == FileBufferType::BLOCK) {
		auto block = reinterpret_cast<Block *>(buffer.get());
		replacement_buffer = make_uniq<Block>(allocator, block->id, alloc_size);
	} else {
		replacement_buffer = make_uniq<FileBuffer>(allocator, buffer->GetBufferType(), alloc_size);
	}
	memcpy(replacement_buffer->buffer, buffer->buffer, buffer->size);
	WriteGarbageIntoBuffer(lock, *handle);
	buffer = std::move(replacement_buffer);
#endif
}

void StandardBufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
	bool purge = false;
	{
		auto lock = handle->GetLock();
		if (!handle->GetBuffer(lock) || handle->GetBufferType() == FileBufferType::TINY_BUFFER) {
			return;
		}
		D_ASSERT(handle->Readers() > 0);
		auto new_readers = handle->DecrementReaders();
		if (new_readers == 0) {
			VerifyZeroReaders(lock, handle);
			if (handle->MustAddToEvictionQueue()) {
				purge = buffer_pool.AddToEvictionQueue(handle);
			} else {
				handle->Unload(lock);
			}
		}
	}

	// We do not have to keep the handle locked while purging.
	if (purge) {
		PurgeQueue(*handle);
	}
}

void StandardBufferManager::SetMemoryLimit(idx_t limit) {
	buffer_pool.SetLimit(limit, InMemoryWarning());
}

void StandardBufferManager::SetSwapLimit(optional_idx limit) {
	lock_guard<mutex> guard(temporary_directory.lock);
	if (temporary_directory.handle) {
		temporary_directory.handle->GetTempFile().SetMaxSwapSpace(limit);
	} else {
		temporary_directory.maximum_swap_space = limit;
	}
}

vector<MemoryInformation> StandardBufferManager::GetMemoryUsageInfo() const {
	vector<MemoryInformation> result;
	for (idx_t k = 0; k < MEMORY_TAG_COUNT; k++) {
		MemoryInformation info;
		info.tag = MemoryTag(k);
		info.size = buffer_pool.memory_usage.GetUsedMemory(MemoryTag(k), BufferPool::MemoryUsageCaches::FLUSH);
		info.evicted_data = evicted_data_per_tag[k].load();
		result.push_back(info);
	}
	return result;
}

unique_ptr<FileBuffer> StandardBufferManager::ReadTemporaryBufferInternal(BufferManager &buffer_manager,
                                                                          FileHandle &handle, idx_t position,
                                                                          idx_t size,
                                                                          unique_ptr<FileBuffer> reusable_buffer) {
	auto buffer = buffer_manager.ConstructManagedBuffer(size, std::move(reusable_buffer));
	buffer->Read(handle, position);
	return buffer;
}

string StandardBufferManager::GetTemporaryPath(block_id_t id) {
	auto &fs = FileSystem::GetFileSystem(db);
	return fs.JoinPath(temporary_directory.path, "duckdb_temp_block-" + to_string(id) + ".block");
}

void StandardBufferManager::RequireTemporaryDirectory() {
	if (temporary_directory.path.empty()) {
		throw InvalidInputException(
		    "Out-of-memory: cannot write buffer because no temporary directory is specified!\nTo enable "
		    "temporary buffer eviction set a temporary directory using PRAGMA temp_directory='/path/to/tmp.tmp'");
	}
	lock_guard<mutex> guard(temporary_directory.lock);
	if (!temporary_directory.handle) {
		// temp directory has not been created yet: initialize it
		temporary_directory.handle =
		    make_uniq<TemporaryDirectoryHandle>(db, temporary_directory.path, temporary_directory.maximum_swap_space);
	}
}

void StandardBufferManager::WriteTemporaryBuffer(MemoryTag tag, block_id_t block_id, FileBuffer &buffer) {

	// WriteTemporaryBuffer assumes that we never write a buffer below DEFAULT_BLOCK_ALLOC_SIZE.
	RequireTemporaryDirectory();

	// Append to a few grouped files.
	if (buffer.AllocSize() == GetBlockAllocSize()) {
		evicted_data_per_tag[uint8_t(tag)] += GetBlockAllocSize();
		temporary_directory.handle->GetTempFile().WriteTemporaryBuffer(block_id, buffer);
		return;
	}

	// Get the path to write to.
	auto path = GetTemporaryPath(block_id);
	evicted_data_per_tag[uint8_t(tag)] += buffer.AllocSize();

	// Create the file and write the size followed by the buffer contents.
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	temporary_directory.handle->GetTempFile().IncreaseSizeOnDisk(buffer.AllocSize() + sizeof(idx_t));
	handle->Write(&buffer.size, sizeof(idx_t), 0);
	buffer.Write(*handle, sizeof(idx_t));
}

unique_ptr<FileBuffer> StandardBufferManager::ReadTemporaryBuffer(MemoryTag tag, BlockHandle &block,
                                                                  unique_ptr<FileBuffer> reusable_buffer) {
	D_ASSERT(!temporary_directory.path.empty());
	D_ASSERT(temporary_directory.handle.get());
	auto id = block.BlockId();
	if (temporary_directory.handle->GetTempFile().HasTemporaryBuffer(id)) {
		// This is a block that was offloaded to a regular .tmp file, the file contains blocks of a fixed size
		return temporary_directory.handle->GetTempFile().ReadTemporaryBuffer(id, std::move(reusable_buffer));
	}

	// This block contains data of variable size so we need to open it and read it to get its size.
	idx_t block_size;
	auto path = GetTemporaryPath(id);
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	handle->Read(&block_size, sizeof(idx_t), 0);

	// Allocate a buffer of the file's size and read the data into that buffer.
	auto buffer = ReadTemporaryBufferInternal(*this, *handle, sizeof(idx_t), block_size, std::move(reusable_buffer));
	handle.reset();

	// Delete the file and return the buffer.
	DeleteTemporaryFile(block);
	return buffer;
}

void StandardBufferManager::DeleteTemporaryFile(BlockHandle &block) {
	auto id = block.BlockId();
	if (temporary_directory.path.empty()) {
		// no temporary directory specified: nothing to delete
		return;
	}
	{
		lock_guard<mutex> guard(temporary_directory.lock);
		if (!temporary_directory.handle) {
			// temporary directory was not initialized yet: nothing to delete
			return;
		}
	}
	// check if we should delete the file from the shared pool of files, or from the general file system
	if (temporary_directory.handle->GetTempFile().HasTemporaryBuffer(id)) {
		evicted_data_per_tag[uint8_t(block.GetMemoryTag())] -= GetBlockAllocSize();
		temporary_directory.handle->GetTempFile().DeleteTemporaryBuffer(id);
		return;
	}

	// The file is not in the shared pool of files.
	auto &fs = FileSystem::GetFileSystem(db);
	auto path = GetTemporaryPath(id);
	if (fs.FileExists(path)) {
		evicted_data_per_tag[uint8_t(block.GetMemoryTag())] -= block.GetMemoryUsage();
		auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
		auto content_size = handle->GetFileSize();
		handle.reset();
		fs.RemoveFile(path);
		temporary_directory.handle->GetTempFile().DecreaseSizeOnDisk(content_size);
	}
}

bool StandardBufferManager::HasTemporaryDirectory() const {
	return !temporary_directory.path.empty();
}

vector<TemporaryFileInformation> StandardBufferManager::GetTemporaryFiles() {
	vector<TemporaryFileInformation> result;
	if (temporary_directory.path.empty()) {
		return result;
	}
	{
		lock_guard<mutex> temp_handle_guard(temporary_directory.lock);
		if (temporary_directory.handle) {
			result = temporary_directory.handle->GetTempFile().GetTemporaryFiles();
		}
	}
	auto &fs = FileSystem::GetFileSystem(db);
	fs.ListFiles(temporary_directory.path, [&](const string &name, bool is_dir) {
		if (is_dir) {
			return;
		}
		if (!StringUtil::EndsWith(name, ".block")) {
			return;
		}

		// Another process or thread can delete the file before we can get its file size.
		auto handle = fs.OpenFile(name, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		if (!handle) {
			return;
		}

		TemporaryFileInformation info;
		info.path = name;
		info.size = NumericCast<idx_t>(fs.GetFileSize(*handle));
		handle.reset();
		result.push_back(info);
	});
	return result;
}

const char *StandardBufferManager::InMemoryWarning() {
	if (!temporary_directory.path.empty()) {
		return "";
	}
	return "\nDatabase is launched in in-memory mode and no temporary directory is specified."
	       "\nUnused blocks cannot be offloaded to disk."
	       "\n\nLaunch the database with a persistent storage back-end"
	       "\nOr set SET temp_directory='/path/to/tmp.tmp'";
}

void StandardBufferManager::ReserveMemory(idx_t size) {
	if (size == 0) {
		return;
	}
	auto reservation =
	    EvictBlocksOrThrow(MemoryTag::EXTENSION, size, nullptr, "failed to reserve memory data of size %s%s",
	                       StringUtil::BytesToHumanReadableString(size));
	reservation.size = 0;
}

void StandardBufferManager::FreeReservedMemory(idx_t size) {
	if (size == 0) {
		return;
	}
	buffer_pool.memory_usage.UpdateUsedMemory(MemoryTag::EXTENSION, -(int64_t)size);
}

//===--------------------------------------------------------------------===//
// Buffer Allocator
//===--------------------------------------------------------------------===//
data_ptr_t StandardBufferManager::BufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
	auto &data = private_data->Cast<BufferAllocatorData>();
	auto reservation =
	    data.manager.EvictBlocksOrThrow(MemoryTag::ALLOCATOR, size, nullptr, "failed to allocate data of size %s%s",
	                                    StringUtil::BytesToHumanReadableString(size));
	// We rely on manual tracking of this one. :(
	reservation.size = 0;
	return Allocator::Get(data.manager.db).AllocateData(size);
}

void StandardBufferManager::BufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	auto &data = private_data->Cast<BufferAllocatorData>();
	BufferPoolReservation r(MemoryTag::ALLOCATOR, data.manager.GetBufferPool());
	r.size = size;
	r.Resize(0);
	return Allocator::Get(data.manager.db).FreeData(pointer, size);
}

data_ptr_t StandardBufferManager::BufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer,
                                                         idx_t old_size, idx_t size) {
	if (old_size == size) {
		return pointer;
	}
	auto &data = private_data->Cast<BufferAllocatorData>();
	BufferPoolReservation r(MemoryTag::ALLOCATOR, data.manager.GetBufferPool());
	r.size = old_size;
	r.Resize(size);
	r.size = 0;
	return Allocator::Get(data.manager.db).ReallocateData(pointer, old_size, size);
}

Allocator &BufferAllocator::Get(ClientContext &context) {
	auto &manager = StandardBufferManager::GetBufferManager(context);
	return manager.GetBufferAllocator();
}

Allocator &BufferAllocator::Get(DatabaseInstance &db) {
	return StandardBufferManager::GetBufferManager(db).GetBufferAllocator();
}

Allocator &BufferAllocator::Get(AttachedDatabase &db) {
	return BufferAllocator::Get(db.GetDatabase());
}

Allocator &StandardBufferManager::GetBufferAllocator() {
	return buffer_allocator;
}

} // namespace duckdb
