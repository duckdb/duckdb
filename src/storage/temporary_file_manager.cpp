#include "duckdb/storage/temporary_file_manager.hpp"
#include "duckdb/storage/buffer/temporary_file_information.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// BlockIndexManager
//===--------------------------------------------------------------------===//

BlockIndexManager::BlockIndexManager(TemporaryFileManager &manager) : max_index(0), manager(&manager) {
}

BlockIndexManager::BlockIndexManager() : max_index(0), manager(nullptr) {
}

idx_t BlockIndexManager::GetNewBlockIndex() {
	auto index = GetNewBlockIndexInternal();
	indexes_in_use.insert(index);
	return index;
}

bool BlockIndexManager::RemoveIndex(idx_t index) {
	// remove this block from the set of blocks
	auto entry = indexes_in_use.find(index);
	if (entry == indexes_in_use.end()) {
		throw InternalException("RemoveIndex - index %llu not found in indexes_in_use", index);
	}
	indexes_in_use.erase(entry);
	free_indexes.insert(index);
	// check if we can truncate the file

	// get the max_index in use right now
	auto max_index_in_use = indexes_in_use.empty() ? 0 : *indexes_in_use.rbegin() + 1;
	if (max_index_in_use < max_index) {
		// max index in use is lower than the max_index
		// reduce the max_index
		SetMaxIndex(max_index_in_use);
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

idx_t BlockIndexManager::GetMaxIndex() {
	return max_index;
}

bool BlockIndexManager::HasFreeBlocks() {
	return !free_indexes.empty();
}

void BlockIndexManager::SetMaxIndex(idx_t new_index) {
	static constexpr idx_t TEMP_FILE_BLOCK_SIZE = DEFAULT_BLOCK_ALLOC_SIZE;
	if (!manager) {
		max_index = new_index;
	} else {
		auto old = max_index;
		if (new_index < old) {
			max_index = new_index;
			auto difference = old - new_index;
			auto size_on_disk = difference * TEMP_FILE_BLOCK_SIZE;
			manager->DecreaseSizeOnDisk(size_on_disk);
		} else if (new_index > old) {
			auto difference = new_index - old;
			auto size_on_disk = difference * TEMP_FILE_BLOCK_SIZE;
			manager->IncreaseSizeOnDisk(size_on_disk);
			// Increase can throw, so this is only updated after it was succesfully updated
			max_index = new_index;
		}
	}
}

idx_t BlockIndexManager::GetNewBlockIndexInternal() {
	if (free_indexes.empty()) {
		auto new_index = max_index;
		SetMaxIndex(max_index + 1);
		return new_index;
	}
	auto entry = free_indexes.begin();
	auto index = *entry;
	free_indexes.erase(entry);
	return index;
}

//===--------------------------------------------------------------------===//
// TemporaryFileHandle
//===--------------------------------------------------------------------===//

TemporaryFileHandle::TemporaryFileHandle(idx_t temp_file_count, DatabaseInstance &db, const string &temp_directory,
                                         idx_t index, TemporaryFileManager &manager)
    : max_allowed_index((1 << temp_file_count) * MAX_ALLOWED_INDEX_BASE), db(db), file_index(index),
      path(FileSystem::GetFileSystem(db).JoinPath(temp_directory, "duckdb_temp_storage-" + to_string(index) + ".tmp")),
      index_manager(manager) {
}

TemporaryFileHandle::TemporaryFileLock::TemporaryFileLock(mutex &mutex) : lock(mutex) {
}

TemporaryFileIndex TemporaryFileHandle::TryGetBlockIndex() {
	TemporaryFileLock lock(file_lock);
	if (index_manager.GetMaxIndex() >= max_allowed_index && index_manager.HasFreeBlocks()) {
		// file is at capacity
		return TemporaryFileIndex();
	}
	// open the file handle if it does not yet exist
	CreateFileIfNotExists(lock);
	// fetch a new block index to write to
	auto block_index = index_manager.GetNewBlockIndex();
	return TemporaryFileIndex(file_index, block_index);
}

void TemporaryFileHandle::WriteTemporaryFile(FileBuffer &buffer, TemporaryFileIndex index) {
	// We group DEFAULT_BLOCK_ALLOC_SIZE blocks into the same file.
	D_ASSERT(buffer.size == BufferManager::GetBufferManager(db).GetBlockSize());
	buffer.Write(*handle, GetPositionInFile(index.block_index));
}

unique_ptr<FileBuffer> TemporaryFileHandle::ReadTemporaryBuffer(idx_t block_index,
                                                                unique_ptr<FileBuffer> reusable_buffer) {
	return StandardBufferManager::ReadTemporaryBufferInternal(
	    BufferManager::GetBufferManager(db), *handle, GetPositionInFile(block_index),
	    BufferManager::GetBufferManager(db).GetBlockSize(), std::move(reusable_buffer));
}

void TemporaryFileHandle::EraseBlockIndex(block_id_t block_index) {
	// remove the block (and potentially truncate the temp file)
	TemporaryFileLock lock(file_lock);
	D_ASSERT(handle);
	RemoveTempBlockIndex(lock, NumericCast<idx_t>(block_index));
}

bool TemporaryFileHandle::DeleteIfEmpty() {
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

TemporaryFileInformation TemporaryFileHandle::GetTemporaryFile() {
	TemporaryFileLock lock(file_lock);
	TemporaryFileInformation info;
	info.path = path;
	info.size = GetPositionInFile(index_manager.GetMaxIndex());
	return info;
}

void TemporaryFileHandle::CreateFileIfNotExists(TemporaryFileLock &) {
	if (handle) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(db);
	auto open_flags = FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;
	handle = fs.OpenFile(path, open_flags);
}

void TemporaryFileHandle::RemoveTempBlockIndex(TemporaryFileLock &, idx_t index) {
	// remove the block index from the index manager
	if (index_manager.RemoveIndex(index)) {
		// the max_index that is currently in use has decreased
		// as a result we can truncate the file
#ifndef WIN32 // this ended up causing issues when sorting
		auto max_index = index_manager.GetMaxIndex();
		auto &fs = FileSystem::GetFileSystem(db);
		fs.Truncate(*handle, NumericCast<int64_t>(GetPositionInFile(max_index + 1)));
#endif
	}
}

idx_t TemporaryFileHandle::GetPositionInFile(idx_t index) {
	return index * BufferManager::GetBufferManager(db).GetBlockAllocSize();
}

//===--------------------------------------------------------------------===//
// TemporaryDirectoryHandle
//===--------------------------------------------------------------------===//

TemporaryDirectoryHandle::TemporaryDirectoryHandle(DatabaseInstance &db, string path_p, optional_idx max_swap_space)
    : db(db), temp_directory(std::move(path_p)), temp_file(make_uniq<TemporaryFileManager>(db, temp_directory)) {
	auto &fs = FileSystem::GetFileSystem(db);
	D_ASSERT(!temp_directory.empty());
	if (!fs.DirectoryExists(temp_directory)) {
		fs.CreateDirectory(temp_directory);
		created_directory = true;
	}
	temp_file->SetMaxSwapSpace(max_swap_space);
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

//===--------------------------------------------------------------------===//
// TemporaryFileIndex
//===--------------------------------------------------------------------===//

TemporaryFileIndex::TemporaryFileIndex(idx_t file_index, idx_t block_index)
    : file_index(file_index), block_index(block_index) {
}

bool TemporaryFileIndex::IsValid() const {
	return block_index != DConstants::INVALID_INDEX;
}

//===--------------------------------------------------------------------===//
// TemporaryFileManager
//===--------------------------------------------------------------------===//

static idx_t GetDefaultMax(const string &path) {
	D_ASSERT(!path.empty());
	auto disk_space = FileSystem::GetAvailableDiskSpace(path);
	// Use the available disk space
	// We have made sure that the file exists before we call this, it shouldn't fail
	if (!disk_space.IsValid()) {
		// But if it does (i.e because the system call is not implemented)
		// we don't cap the available swap space
		return DConstants::INVALID_INDEX - 1;
	}
	// Only use 90% of the available disk space
	return static_cast<idx_t>(static_cast<double>(disk_space.GetIndex()) * 0.9);
}

TemporaryFileManager::TemporaryFileManager(DatabaseInstance &db, const string &temp_directory_p)
    : db(db), temp_directory(temp_directory_p), size_on_disk(0), max_swap_space(0) {
}

TemporaryFileManager::~TemporaryFileManager() {
	files.clear();
}

TemporaryFileManager::TemporaryManagerLock::TemporaryManagerLock(mutex &mutex) : lock(mutex) {
}

void TemporaryFileManager::WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer) {
	// We group DEFAULT_BLOCK_ALLOC_SIZE blocks into the same file.
	D_ASSERT(buffer.size == BufferManager::GetBufferManager(db).GetBlockSize());
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
			auto new_file = make_uniq<TemporaryFileHandle>(files.size(), db, temp_directory, new_file_index, *this);
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

bool TemporaryFileManager::HasTemporaryBuffer(block_id_t block_id) {
	lock_guard<mutex> lock(manager_lock);
	return used_blocks.find(block_id) != used_blocks.end();
}

idx_t TemporaryFileManager::GetTotalUsedSpaceInBytes() {
	return size_on_disk.load();
}

optional_idx TemporaryFileManager::GetMaxSwapSpace() const {
	return max_swap_space;
}

void TemporaryFileManager::SetMaxSwapSpace(optional_idx limit) {
	idx_t new_limit;
	if (limit.IsValid()) {
		new_limit = limit.GetIndex();
	} else {
		new_limit = GetDefaultMax(temp_directory);
	}

	auto current_size_on_disk = size_on_disk.load();
	if (current_size_on_disk > new_limit) {
		auto used = StringUtil::BytesToHumanReadableString(current_size_on_disk);
		auto max = StringUtil::BytesToHumanReadableString(new_limit);
		throw OutOfMemoryException(
		    R"(failed to adjust the 'max_temp_directory_size', currently used space (%s) exceeds the new limit (%s)
Please increase the limit or destroy the buffers stored in the temp directory by e.g removing temporary tables.
To get usage information of the temp_directory, use 'CALL duckdb_temporary_files();'
		)",
		    used, max);
	}
	max_swap_space = new_limit;
}

void TemporaryFileManager::IncreaseSizeOnDisk(idx_t bytes) {
	auto current_size_on_disk = size_on_disk.load();
	if (current_size_on_disk + bytes > max_swap_space) {
		auto used = StringUtil::BytesToHumanReadableString(current_size_on_disk);
		auto max = StringUtil::BytesToHumanReadableString(max_swap_space);
		auto data_size = StringUtil::BytesToHumanReadableString(bytes);
		throw OutOfMemoryException(R"(failed to offload data block of size %s (%s/%s used).
This limit was set by the 'max_temp_directory_size' setting.
By default, this setting utilizes the available disk space on the drive where the 'temp_directory' is located.
You can adjust this setting, by using (for example) PRAGMA max_temp_directory_size='10GiB')",
		                           data_size, used, max);
	}
	size_on_disk += bytes;
}

void TemporaryFileManager::DecreaseSizeOnDisk(idx_t bytes) {
	size_on_disk -= bytes;
}

unique_ptr<FileBuffer> TemporaryFileManager::ReadTemporaryBuffer(block_id_t id,
                                                                 unique_ptr<FileBuffer> reusable_buffer) {
	TemporaryFileIndex index;
	TemporaryFileHandle *handle;
	{
		TemporaryManagerLock lock(manager_lock);
		index = GetTempBlockIndex(lock, id);
		handle = GetFileHandle(lock, index.file_index);
	}
	auto buffer = handle->ReadTemporaryBuffer(index.block_index, std::move(reusable_buffer));
	{
		// remove the block (and potentially erase the temp file)
		TemporaryManagerLock lock(manager_lock);
		EraseUsedBlock(lock, id, handle, index);
	}
	return buffer;
}

void TemporaryFileManager::DeleteTemporaryBuffer(block_id_t id) {
	TemporaryManagerLock lock(manager_lock);
	auto index = GetTempBlockIndex(lock, id);
	auto handle = GetFileHandle(lock, index.file_index);
	EraseUsedBlock(lock, id, handle, index);
}

vector<TemporaryFileInformation> TemporaryFileManager::GetTemporaryFiles() {
	lock_guard<mutex> lock(manager_lock);
	vector<TemporaryFileInformation> result;
	for (auto &file : files) {
		result.push_back(file.second->GetTemporaryFile());
	}
	return result;
}

void TemporaryFileManager::EraseUsedBlock(TemporaryManagerLock &lock, block_id_t id, TemporaryFileHandle *handle,
                                          TemporaryFileIndex index) {
	auto entry = used_blocks.find(id);
	if (entry == used_blocks.end()) {
		throw InternalException("EraseUsedBlock - Block %llu not found in used blocks", id);
	}
	used_blocks.erase(entry);
	handle->EraseBlockIndex(NumericCast<block_id_t>(index.block_index));
	if (handle->DeleteIfEmpty()) {
		EraseFileHandle(lock, index.file_index);
	}
}

// FIXME: returning a raw pointer???
TemporaryFileHandle *TemporaryFileManager::GetFileHandle(TemporaryManagerLock &, idx_t index) {
	return files[index].get();
}

TemporaryFileIndex TemporaryFileManager::GetTempBlockIndex(TemporaryManagerLock &, block_id_t id) {
	D_ASSERT(used_blocks.find(id) != used_blocks.end());
	return used_blocks[id];
}

void TemporaryFileManager::EraseFileHandle(TemporaryManagerLock &, idx_t file_index) {
	files.erase(file_index);
	index_manager.RemoveIndex(file_index);
}

} // namespace duckdb
