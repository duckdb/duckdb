#include "duckdb/storage/standard_buffer_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"

namespace duckdb {

#ifdef DUCKDB_DEBUG_DESTROY_BLOCKS
static void WriteGarbageIntoBuffer(FileBuffer &buffer) {
	memset(buffer.buffer, 0xa5, buffer.size); // 0xa5 is default memory in debug mode
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
	temporary_directory.path = std::move(tmp);
	temp_block_manager = make_uniq<InMemoryBlockManager>(*this);
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

shared_ptr<BlockHandle> StandardBufferManager::RegisterSmallMemory(idx_t block_size) {
	D_ASSERT(block_size < Storage::BLOCK_SIZE);
	auto reservation =
	    EvictBlocksOrThrow(MemoryTag::BASE_TABLE, block_size, nullptr, "could not allocate block of size %s%s",
	                       StringUtil::BytesToHumanReadableString(block_size));

	auto buffer = ConstructManagedBuffer(block_size, nullptr, FileBufferType::TINY_BUFFER);

	// create a new block pointer for this block
	auto result = make_shared_ptr<BlockHandle>(*temp_block_manager, ++temporary_id, MemoryTag::BASE_TABLE,
	                                           std::move(buffer), false, block_size, std::move(reservation));
#ifdef DUCKDB_DEBUG_DESTROY_BLOCKS
	// Initialize the memory with garbage data
	WriteGarbageIntoBuffer(*result->buffer);
#endif
	return result;
}

shared_ptr<BlockHandle> StandardBufferManager::RegisterMemory(MemoryTag tag, idx_t block_size, bool can_destroy) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	auto alloc_size = GetAllocSize(block_size);
	// first evict blocks until we have enough memory to store this buffer
	unique_ptr<FileBuffer> reusable_buffer;
	auto res = EvictBlocksOrThrow(tag, alloc_size, &reusable_buffer, "could not allocate block of size %s%s",
	                              StringUtil::BytesToHumanReadableString(alloc_size));

	auto buffer = ConstructManagedBuffer(block_size, std::move(reusable_buffer));

	// create a new block pointer for this block
	return make_shared_ptr<BlockHandle>(*temp_block_manager, ++temporary_id, tag, std::move(buffer), can_destroy,
	                                    alloc_size, std::move(res));
}

BufferHandle StandardBufferManager::Allocate(MemoryTag tag, idx_t block_size, bool can_destroy,
                                             shared_ptr<BlockHandle> *block) {
	shared_ptr<BlockHandle> local_block;
	auto block_ptr = block ? block : &local_block;
	*block_ptr = RegisterMemory(tag, block_size, can_destroy);
#ifdef DUCKDB_DEBUG_DESTROY_BLOCKS
	// Initialize the memory with garbage data
	WriteGarbageIntoBuffer(*(*block_ptr)->buffer);
#endif
	return Pin(*block_ptr);
}

void StandardBufferManager::ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	unique_lock<mutex> lock(handle->lock);
	D_ASSERT(handle->state == BlockState::BLOCK_LOADED);
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
	D_ASSERT(handle->memory_usage == handle->memory_charge.size);

	auto req = handle->buffer->CalculateMemory(block_size);
	int64_t memory_delta = NumericCast<int64_t>(req.alloc_size) - NumericCast<int64_t>(handle->memory_usage);

	if (memory_delta == 0) {
		return;
	} else if (memory_delta > 0) {
		// evict blocks until we have space to resize this block
		// unlock the handle lock during the call to EvictBlocksOrThrow
		lock.unlock();
		auto reservation = EvictBlocksOrThrow(handle->tag, NumericCast<idx_t>(memory_delta), nullptr,
		                                      "failed to resize block from %s to %s%s",
		                                      StringUtil::BytesToHumanReadableString(handle->memory_usage),
		                                      StringUtil::BytesToHumanReadableString(req.alloc_size));
		lock.lock();

		// EvictBlocks decrements 'current_memory' for us.
		handle->memory_charge.Merge(std::move(reservation));
	} else {
		// no need to evict blocks, but we do need to decrement 'current_memory'.
		handle->memory_charge.Resize(req.alloc_size);
	}

	handle->ResizeBuffer(block_size, memory_delta);
}

BufferHandle StandardBufferManager::Pin(shared_ptr<BlockHandle> &handle) {
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
	auto reservation =
	    EvictBlocksOrThrow(handle->tag, required_memory, &reusable_buffer, "failed to pin block of size %s%s",
	                       StringUtil::BytesToHumanReadableString(required_memory));
	// lock the handle again and repeat the check (in case anybody loaded in the meantime)
	lock_guard<mutex> lock(handle->lock);
	// check if the block is already loaded
	if (handle->state == BlockState::BLOCK_LOADED) {
		// the block is loaded, increment the reader count and return a pointer to the handle
		handle->readers++;
		reservation.Resize(0);
		return handle->Load(handle);
	}
	// now we can actually load the current block
	D_ASSERT(handle->readers == 0);
	handle->readers = 1;
	auto buf = handle->Load(handle, std::move(reusable_buffer));
	handle->memory_charge = std::move(reservation);
	// In the case of a variable sized block, the buffer may be smaller than a full block.
	int64_t delta = NumericCast<int64_t>(handle->buffer->AllocSize()) - NumericCast<int64_t>(handle->memory_usage);
	if (delta) {
		D_ASSERT(delta < 0);
		handle->memory_usage += NumericCast<idx_t>(delta);
		handle->memory_charge.Resize(handle->memory_usage);
	}
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
	return buf;
}

void StandardBufferManager::PurgeQueue() {
	buffer_pool.PurgeQueue();
}

void StandardBufferManager::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	buffer_pool.AddToEvictionQueue(handle);
}

void StandardBufferManager::VerifyZeroReaders(shared_ptr<BlockHandle> &handle) {
#ifdef DUCKDB_DEBUG_DESTROY_BLOCKS
	auto replacement_buffer = make_uniq<FileBuffer>(Allocator::Get(db), handle->buffer->type,
	                                                handle->memory_usage - Storage::BLOCK_HEADER_SIZE);
	memcpy(replacement_buffer->buffer, handle->buffer->buffer, handle->buffer->size);
	WriteGarbageIntoBuffer(*handle->buffer);
	handle->buffer = std::move(replacement_buffer);
#endif
}

void StandardBufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
	bool purge = false;
	{
		lock_guard<mutex> lock(handle->lock);
		if (!handle->buffer || handle->buffer->type == FileBufferType::TINY_BUFFER) {
			return;
		}
		D_ASSERT(handle->readers > 0);
		handle->readers--;
		if (handle->readers == 0) {
			VerifyZeroReaders(handle);
			purge = buffer_pool.AddToEvictionQueue(handle);
		}
	}

	// We do not have to keep the handle locked while purging.
	if (purge) {
		PurgeQueue();
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
		info.size = buffer_pool.memory_usage_per_tag[k].load();
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
	RequireTemporaryDirectory();
	if (buffer.size == Storage::BLOCK_SIZE) {
		evicted_data_per_tag[uint8_t(tag)] += Storage::BLOCK_SIZE;
		temporary_directory.handle->GetTempFile().WriteTemporaryBuffer(block_id, buffer);
		return;
	}
	evicted_data_per_tag[uint8_t(tag)] += buffer.size;
	// get the path to write to
	auto path = GetTemporaryPath(block_id);
	D_ASSERT(buffer.size > Storage::BLOCK_SIZE);
	// create the file and write the size followed by the buffer contents
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	handle->Write(&buffer.size, sizeof(idx_t), 0);
	buffer.Write(*handle, sizeof(idx_t));
}

unique_ptr<FileBuffer> StandardBufferManager::ReadTemporaryBuffer(MemoryTag tag, block_id_t id,
                                                                  unique_ptr<FileBuffer> reusable_buffer) {
	D_ASSERT(!temporary_directory.path.empty());
	D_ASSERT(temporary_directory.handle.get());
	if (temporary_directory.handle->GetTempFile().HasTemporaryBuffer(id)) {
		evicted_data_per_tag[uint8_t(tag)] -= Storage::BLOCK_SIZE;
		return temporary_directory.handle->GetTempFile().ReadTemporaryBuffer(id, std::move(reusable_buffer));
	}
	idx_t block_size;
	// open the temporary file and read the size
	auto path = GetTemporaryPath(id);
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	handle->Read(&block_size, sizeof(idx_t), 0);
	evicted_data_per_tag[uint8_t(tag)] -= block_size;

	// now allocate a buffer of this size and read the data into that buffer
	auto buffer = ReadTemporaryBufferInternal(*this, *handle, sizeof(idx_t), block_size, std::move(reusable_buffer));

	handle.reset();
	DeleteTemporaryFile(id);
	return buffer;
}

void StandardBufferManager::DeleteTemporaryFile(block_id_t id) {
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
		temporary_directory.handle->GetTempFile().DeleteTemporaryBuffer(id);
		return;
	}
	auto &fs = FileSystem::GetFileSystem(db);
	auto path = GetTemporaryPath(id);
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
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
		TemporaryFileInformation info;
		info.path = name;
		auto handle = fs.OpenFile(name, FileFlags::FILE_FLAGS_READ);
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
	buffer_pool.current_memory -= size;
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
