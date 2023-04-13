#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

struct BufferAllocatorData : PrivateAllocatorData {
	explicit BufferAllocatorData(BufferManager &manager) : manager(manager) {
	}

	BufferManager &manager;
};

idx_t GetAllocSize(idx_t size) {
	return AlignValue<idx_t, Storage::SECTOR_SIZE>(size + Storage::BLOCK_HEADER_SIZE);
}

unique_ptr<FileBuffer> BufferManager::ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&source,
                                                             FileBufferType type) {
	if (source) {
		auto tmp = std::move(source);
		D_ASSERT(tmp->AllocSize() == BufferManager::GetAllocSize(size));
		return make_uniq<FileBuffer>(*tmp, type);
	} else {
		// no re-usable buffer: allocate a new buffer
		return make_uniq<FileBuffer>(Allocator::Get(db), type, size);
	}
}

class TemporaryFileManager;

class TemporaryDirectoryHandle {
public:
	TemporaryDirectoryHandle(DatabaseInstance &db, string path_p);
	~TemporaryDirectoryHandle();

	TemporaryFileManager &GetTempFile();

private:
	DatabaseInstance &db;
	string temp_directory;
	bool created_directory = false;
	unique_ptr<TemporaryFileManager> temp_file;
};

void BufferManager::SetTemporaryDirectory(string new_dir) {
	if (temp_directory_handle) {
		throw NotImplementedException("Cannot switch temporary directory after the current one has been used");
	}
	this->temp_directory = std::move(new_dir);
}

BufferManager::BufferManager(DatabaseInstance &db, string tmp)
    : db(db), buffer_pool(db.GetBufferPool()), temp_directory(std::move(tmp)), temporary_id(MAXIMUM_BLOCK),
      buffer_allocator(BufferAllocatorAllocate, BufferAllocatorFree, BufferAllocatorRealloc,
                       make_uniq<BufferAllocatorData>(*this)) {
	temp_block_manager = make_uniq<InMemoryBlockManager>(*this);
}

BufferManager::~BufferManager() {
}

template <typename... ARGS>
TempBufferPoolReservation BufferManager::EvictBlocksOrThrow(idx_t memory_delta, unique_ptr<FileBuffer> *buffer,
                                                            ARGS... args) {
	auto r = buffer_pool.EvictBlocks(memory_delta, buffer_pool.maximum_memory, buffer);
	if (!r.success) {
		throw OutOfMemoryException(args..., InMemoryWarning());
	}
	return std::move(r.reservation);
}

shared_ptr<BlockHandle> BufferManager::RegisterSmallMemory(idx_t block_size) {
	D_ASSERT(block_size < Storage::BLOCK_SIZE);
	auto res = EvictBlocksOrThrow(block_size, nullptr, "could not allocate block of %lld bytes (%lld/%lld used) %s",
	                              block_size, GetUsedMemory(), GetMaxMemory());

	auto buffer = ConstructManagedBuffer(block_size, nullptr, FileBufferType::TINY_BUFFER);

	// create a new block pointer for this block
	return make_shared<BlockHandle>(*temp_block_manager, ++temporary_id, std::move(buffer), false, block_size,
	                                std::move(res));
}

shared_ptr<BlockHandle> BufferManager::RegisterMemory(idx_t block_size, bool can_destroy) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	auto alloc_size = GetAllocSize(block_size);
	// first evict blocks until we have enough memory to store this buffer
	unique_ptr<FileBuffer> reusable_buffer;
	auto res =
	    EvictBlocksOrThrow(alloc_size, &reusable_buffer, "could not allocate block of %lld bytes (%lld/%lld used) %s",
	                       alloc_size, GetUsedMemory(), GetMaxMemory());

	auto buffer = ConstructManagedBuffer(block_size, std::move(reusable_buffer));

	// create a new block pointer for this block
	return make_shared<BlockHandle>(*temp_block_manager, ++temporary_id, std::move(buffer), can_destroy, alloc_size,
	                                std::move(res));
}

BufferHandle BufferManager::Allocate(idx_t block_size, bool can_destroy, shared_ptr<BlockHandle> *block) {
	shared_ptr<BlockHandle> local_block;
	auto block_ptr = block ? block : &local_block;
	*block_ptr = RegisterMemory(block_size, can_destroy);
	return Pin(*block_ptr);
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
		auto reservation = EvictBlocksOrThrow(memory_delta, nullptr, "failed to resize block from %lld to %lld%s",
		                                      handle->memory_usage, req.alloc_size);
		// EvictBlocks decrements 'current_memory' for us.
		handle->memory_charge.Merge(std::move(reservation));
	} else {
		// no need to evict blocks, but we do need to decrement 'current_memory'.
		handle->memory_charge.Resize(req.alloc_size);
	}

	// resize and adjust current memory
	handle->buffer->Resize(block_size);
	handle->memory_usage += memory_delta;
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
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
	auto reservation =
	    EvictBlocksOrThrow(required_memory, &reusable_buffer, "failed to pin block of size %lld%s", required_memory);
	// lock the handle again and repeat the check (in case anybody loaded in the mean time)
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
	int64_t delta = handle->buffer->AllocSize() - handle->memory_usage;
	if (delta) {
		D_ASSERT(delta < 0);
		handle->memory_usage += delta;
		handle->memory_charge.Resize(handle->memory_usage);
	}
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
	return buf;
}

void BufferManager::VerifyZeroReaders(shared_ptr<BlockHandle> &handle) {
#ifdef DUCKDB_DEBUG_DESTROY_BLOCKS
	auto replacement_buffer = make_uniq<FileBuffer>(Allocator::Get(db), handle->buffer->type,
	                                                handle->memory_usage - Storage::BLOCK_HEADER_SIZE);
	memcpy(replacement_buffer->buffer, handle->buffer->buffer, handle->buffer->size);
	memset(handle->buffer->buffer, 165, handle->buffer->size); // 165 is default memory in debug mode
	handle->buffer = std::move(replacement_buffer);
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
		buffer_pool.AddToEvictionQueue(handle);
	}
}

void BufferManager::IncreaseUsedMemory(idx_t size) {
	ReserveMemory(size);
}

void BufferManager::DecreaseUsedMemory(idx_t size) {
	FreeReservedMemory(size);
}

//===--------------------------------------------------------------------===//
// Temporary File Management
//===--------------------------------------------------------------------===//
unique_ptr<FileBuffer> ReadTemporaryBufferInternal(BufferManager &buffer_manager, FileHandle &handle, idx_t position,
                                                   idx_t size, block_id_t id, unique_ptr<FileBuffer> reusable_buffer) {
	auto buffer = buffer_manager.ConstructManagedBuffer(size, std::move(reusable_buffer));
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
		auto entry = indexes_in_use.find(index);
		if (entry == indexes_in_use.end()) {
			throw InternalException("RemoveIndex - index %llu not found in indexes_in_use", index);
		}
		indexes_in_use.erase(entry);
		free_indexes.insert(index);
		// check if we can truncate the file

		// get the max_index in use right now
		auto max_index_in_use = indexes_in_use.empty() ? 0 : *indexes_in_use.rbegin();
		if (max_index_in_use < max_index) {
			// max index in use is lower than the max_index
			// reduce the max_index
			max_index = indexes_in_use.empty() ? 0 : max_index_in_use + 1;
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
		return ReadTemporaryBufferInternal(BufferManager::GetBufferManager(db), *handle, GetPositionInFile(block_index),
		                                   Storage::BLOCK_SIZE, id, std::move(reusable_buffer));
	}

	void EraseBlockIndex(block_id_t block_index) {
		// remove the block (and potentially truncate the temp file)
		TemporaryFileLock lock(file_lock);
		D_ASSERT(handle);
		RemoveTempBlockIndex(lock, block_index);
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

	TemporaryFileInformation GetTemporaryFile() {
		TemporaryFileLock lock(file_lock);
		TemporaryFileInformation info;
		info.path = path;
		info.size = GetPositionInFile(index_manager.GetMaxIndex());
		return info;
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
#ifndef WIN32 // this ended up causing issues when sorting
			auto max_index = index_manager.GetMaxIndex();
			auto &fs = FileSystem::GetFileSystem(db);
			fs.Truncate(*handle, GetPositionInFile(max_index + 1));
#endif
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
				auto new_file = make_uniq<TemporaryFileHandle>(db, temp_directory, new_file_index);
				handle = new_file.get();
				files[new_file_index] = std::move(new_file);

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
		auto buffer = handle->ReadTemporaryBuffer(id, index.block_index, std::move(reusable_buffer));
		{
			// remove the block (and potentially erase the temp file)
			TemporaryManagerLock lock(manager_lock);
			EraseUsedBlock(lock, id, handle, index);
		}
		return buffer;
	}

	void DeleteTemporaryBuffer(block_id_t id) {
		TemporaryManagerLock lock(manager_lock);
		auto index = GetTempBlockIndex(lock, id);
		auto handle = GetFileHandle(lock, index.file_index);
		EraseUsedBlock(lock, id, handle, index);
	}

	vector<TemporaryFileInformation> GetTemporaryFiles() {
		lock_guard<mutex> lock(manager_lock);
		vector<TemporaryFileInformation> result;
		for (auto &file : files) {
			result.push_back(file.second->GetTemporaryFile());
		}
		return result;
	}

private:
	void EraseUsedBlock(TemporaryManagerLock &lock, block_id_t id, TemporaryFileHandle *handle,
	                    TemporaryFileIndex index) {
		auto entry = used_blocks.find(id);
		if (entry == used_blocks.end()) {
			throw InternalException("EraseUsedBlock - Block %llu not found in used blocks", id);
		}
		used_blocks.erase(entry);
		handle->EraseBlockIndex(index.block_index);
		if (handle->DeleteIfEmpty()) {
			EraseFileHandle(lock, index.file_index);
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
    : db(db), temp_directory(std::move(path_p)), temp_file(make_uniq<TemporaryFileManager>(db, temp_directory)) {
	auto &fs = FileSystem::GetFileSystem(db);
	if (!temp_directory.empty()) {
		if (!fs.DirectoryExists(temp_directory)) {
			fs.CreateDirectory(temp_directory);
			created_directory = true;
		}
	}
}
TemporaryDirectoryHandle::~TemporaryDirectoryHandle() {
	// first release any temporary files
	temp_file.reset();
	// then delete the temporary file directory
	auto &fs = FileSystem::GetFileSystem(db);
	if (!temp_directory.empty()) {
		bool delete_directory = created_directory;
		vector<string> files_to_delete;
		if (!created_directory) {
			bool deleted_everything = true;
			fs.ListFiles(temp_directory, [&](const string &path, bool isdir) {
				if (isdir) {
					deleted_everything = false;
					return;
				}
				if (!StringUtil::StartsWith(path, "duckdb_temp_")) {
					deleted_everything = false;
					return;
				}
				files_to_delete.push_back(path);
			});
		}
		if (delete_directory) {
			// we want to remove all files in the directory
			fs.RemoveDirectory(temp_directory);
		} else {
			for (auto &file : files_to_delete) {
				fs.RemoveFile(fs.JoinPath(temp_directory, file));
			}
		}
	}
}

TemporaryFileManager &TemporaryDirectoryHandle::GetTempFile() {
	return *temp_file;
}

string BufferManager::GetTemporaryPath(block_id_t id) {
	auto &fs = FileSystem::GetFileSystem(db);
	return fs.JoinPath(temp_directory, "duckdb_temp_block-" + to_string(id) + ".block");
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
		temp_directory_handle = make_uniq<TemporaryDirectoryHandle>(db, temp_directory);
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
		return temp_directory_handle->GetTempFile().ReadTemporaryBuffer(id, std::move(reusable_buffer));
	}
	idx_t block_size;
	// open the temporary file and read the size
	auto path = GetTemporaryPath(id);
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	handle->Read(&block_size, sizeof(idx_t), 0);

	// now allocate a buffer of this size and read the data into that buffer
	auto buffer =
	    ReadTemporaryBufferInternal(*this, *handle, sizeof(idx_t), block_size, id, std::move(reusable_buffer));

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

vector<TemporaryFileInformation> BufferManager::GetTemporaryFiles() {
	vector<TemporaryFileInformation> result;
	if (temp_directory.empty()) {
		return result;
	}
	{
		lock_guard<mutex> temp_handle_guard(temp_handle_lock);
		if (temp_directory_handle) {
			result = temp_directory_handle->GetTempFile().GetTemporaryFiles();
		}
	}
	auto &fs = FileSystem::GetFileSystem(db);
	fs.ListFiles(temp_directory, [&](const string &name, bool is_dir) {
		if (is_dir) {
			return;
		}
		if (!StringUtil::EndsWith(name, ".block")) {
			return;
		}
		TemporaryFileInformation info;
		info.path = name;
		auto handle = fs.OpenFile(name, FileFlags::FILE_FLAGS_READ);
		info.size = fs.GetFileSize(*handle);
		handle.reset();
		result.push_back(info);
	});
	return result;
}

const char *BufferManager::InMemoryWarning() {
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
	auto reservation = EvictBlocksOrThrow(size, nullptr, "failed to reserve memory data of size %lld%s", size);
	reservation.size = 0;
}

void BufferManager::FreeReservedMemory(idx_t size) {
	if (size == 0) {
		return;
	}
	buffer_pool.current_memory -= size;
}

//===--------------------------------------------------------------------===//
// Buffer Allocator
//===--------------------------------------------------------------------===//
data_ptr_t BufferManager::BufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
	auto &data = (BufferAllocatorData &)*private_data;
	auto reservation = data.manager.EvictBlocksOrThrow(size, nullptr, "failed to allocate data of size %lld%s", size);
	// We rely on manual tracking of this one. :(
	reservation.size = 0;
	return Allocator::Get(data.manager.db).AllocateData(size);
}

void BufferManager::BufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	auto &data = (BufferAllocatorData &)*private_data;
	BufferPoolReservation r(data.manager.GetBufferPool());
	r.size = size;
	r.Resize(0);
	return Allocator::Get(data.manager.db).FreeData(pointer, size);
}

data_ptr_t BufferManager::BufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                                 idx_t size) {
	if (old_size == size) {
		return pointer;
	}
	auto &data = (BufferAllocatorData &)*private_data;
	BufferPoolReservation r(data.manager.GetBufferPool());
	r.size = old_size;
	r.Resize(size);
	r.size = 0;
	return Allocator::Get(data.manager.db).ReallocateData(pointer, old_size, size);
}

Allocator &BufferAllocator::Get(ClientContext &context) {
	auto &manager = BufferManager::GetBufferManager(context);
	return manager.GetBufferAllocator();
}

Allocator &BufferAllocator::Get(DatabaseInstance &db) {
	return BufferManager::GetBufferManager(db).GetBufferAllocator();
}

Allocator &BufferAllocator::Get(AttachedDatabase &db) {
	return BufferAllocator::Get(db.GetDatabase());
}

Allocator &BufferManager::GetBufferAllocator() {
	return buffer_allocator;
}

} // namespace duckdb
