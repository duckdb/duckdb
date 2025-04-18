#include "duckdb/storage/temporary_file_manager.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer/temporary_file_information.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"
#include "zstd.h"

namespace duckdb {

//===--------------------------------------------------------------------===//
// TemporaryBufferSize
//===--------------------------------------------------------------------===//
bool TemporaryBufferSizeIsValid(const TemporaryBufferSize size) {
	switch (size) {
	case TemporaryBufferSize::S32K:
	case TemporaryBufferSize::S64K:
	case TemporaryBufferSize::S96K:
	case TemporaryBufferSize::S128K:
	case TemporaryBufferSize::S160K:
	case TemporaryBufferSize::S192K:
	case TemporaryBufferSize::S224K:
	case TemporaryBufferSize::DEFAULT:
		return true;
	default:
		return false;
	}
}

static TemporaryBufferSize SizeToTemporaryBufferSize(const idx_t size) {
	D_ASSERT(size != 0 && size % TEMPORARY_BUFFER_SIZE_GRANULARITY == 0);
	const auto res = static_cast<TemporaryBufferSize>(size);
	D_ASSERT(TemporaryBufferSizeIsValid(res));
	return res;
}

static idx_t TemporaryBufferSizeToSize(const TemporaryBufferSize size) {
	D_ASSERT(TemporaryBufferSizeIsValid(size));
	return static_cast<idx_t>(size);
}

static TemporaryBufferSize RoundUpSizeToTemporaryBufferSize(const idx_t size) {
	return SizeToTemporaryBufferSize(AlignValue<idx_t, TEMPORARY_BUFFER_SIZE_GRANULARITY>(size));
}

static const vector<TemporaryBufferSize> TemporaryBufferSizes() {
	return {TemporaryBufferSize::S32K,  TemporaryBufferSize::S64K,   TemporaryBufferSize::S96K,
	        TemporaryBufferSize::S128K, TemporaryBufferSize::S160K,  TemporaryBufferSize::S192K,
	        TemporaryBufferSize::S224K, TemporaryBufferSize::DEFAULT};
}

static TemporaryBufferSize MinimumCompressedTemporaryBufferSize() {
	return TemporaryBufferSize::S32K;
}

static TemporaryBufferSize MaximumCompressedTemporaryBufferSize() {
	return TemporaryBufferSize::S224K;
}

//===--------------------------------------------------------------------===//
// TemporaryFileIdentifier/TemporaryFileIndex
//===--------------------------------------------------------------------===//
TemporaryFileIdentifier::TemporaryFileIdentifier() : size(TemporaryBufferSize::INVALID) {
}

TemporaryFileIdentifier::TemporaryFileIdentifier(TemporaryBufferSize size_p, idx_t file_index_p)
    : size(size_p), file_index(file_index_p) {
}

bool TemporaryFileIdentifier::IsValid() const {
	return size != TemporaryBufferSize::INVALID && file_index.IsValid();
}

TemporaryFileIndex::TemporaryFileIndex() {
}

TemporaryFileIndex::TemporaryFileIndex(TemporaryFileIdentifier identifier_p, idx_t block_index_p)
    : identifier(identifier_p), block_index(block_index_p) {
}

bool TemporaryFileIndex::IsValid() const {
	return identifier.IsValid() && block_index.IsValid();
}

//===--------------------------------------------------------------------===//
// BlockIndexManager
//===--------------------------------------------------------------------===//
BlockIndexManager::BlockIndexManager() : max_index(0), manager(nullptr) {
}

BlockIndexManager::BlockIndexManager(TemporaryFileManager &manager) : max_index(0), manager(&manager) {
}

idx_t BlockIndexManager::GetNewBlockIndex(const TemporaryBufferSize size) {
	auto index = GetNewBlockIndexInternal(size);
	indexes_in_use.insert(index);
	return index;
}

bool BlockIndexManager::RemoveIndex(idx_t index, const TemporaryBufferSize size) {
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
		SetMaxIndex(max_index_in_use, size);
		// we can remove any free_indexes that are larger than the current max_index
		while (HasFreeBlocks()) {
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

idx_t BlockIndexManager::GetMaxIndex() const {
	return max_index;
}

bool BlockIndexManager::HasFreeBlocks() const {
	return !free_indexes.empty();
}

idx_t BlockIndexManager::GetNewBlockIndexInternal(const TemporaryBufferSize size) {
	if (!HasFreeBlocks()) {
		auto new_index = max_index;
		SetMaxIndex(max_index + 1, size);
		return new_index;
	}
	auto entry = free_indexes.begin();
	auto index = *entry;
	free_indexes.erase(entry);
	return index;
}

void BlockIndexManager::SetMaxIndex(const idx_t new_index, const TemporaryBufferSize size) {
	const auto temp_file_block_size =
	    size == TemporaryBufferSize::DEFAULT ? DEFAULT_BLOCK_ALLOC_SIZE : TemporaryBufferSizeToSize(size);
	if (!manager) {
		max_index = new_index;
	} else {
		auto old = max_index;
		if (new_index < old) {
			max_index = new_index;
			const auto difference = old - new_index;
			const auto size_on_disk = difference * temp_file_block_size;
			manager->DecreaseSizeOnDisk(size_on_disk);
		} else if (new_index > old) {
			const auto difference = new_index - old;
			const auto size_on_disk = difference * temp_file_block_size;
			manager->IncreaseSizeOnDisk(size_on_disk);
			// Increase can throw, so this is only updated after it was successfully updated
			max_index = new_index;
		}
	}
}

//===--------------------------------------------------------------------===//
// TemporaryFileHandle
//===--------------------------------------------------------------------===//
TemporaryFileHandle::TemporaryFileHandle(TemporaryFileManager &manager, TemporaryFileIdentifier identifier_p,
                                         idx_t temp_file_count)
    : db(manager.db), identifier(identifier_p), max_allowed_index((1 << temp_file_count) * MAX_ALLOWED_INDEX_BASE),
      path(manager.CreateTemporaryFileName(identifier)), index_manager(manager) {
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
	auto block_index = index_manager.GetNewBlockIndex(identifier.size);
	return TemporaryFileIndex(identifier, block_index);
}

unique_ptr<FileBuffer> TemporaryFileHandle::ReadTemporaryBuffer(idx_t block_index,
                                                                unique_ptr<FileBuffer> reusable_buffer) const {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (identifier.size == TemporaryBufferSize::DEFAULT) {
		return StandardBufferManager::ReadTemporaryBufferInternal(
		    buffer_manager, *handle, GetPositionInFile(block_index), buffer_manager.GetBlockSize(),
		    std::move(reusable_buffer));
	}

	// Read compressed buffer
	auto compressed_buffer = Allocator::Get(db).Allocate(TemporaryBufferSizeToSize(identifier.size));
	handle->Read(compressed_buffer.get(), compressed_buffer.GetSize(), GetPositionInFile(block_index));

	// Decompress into buffer
	auto buffer = buffer_manager.ConstructManagedBuffer(buffer_manager.GetBlockSize(), std::move(reusable_buffer));

	const auto compressed_size = Load<idx_t>(compressed_buffer.get());
	D_ASSERT(!duckdb_zstd::ZSTD_isError(compressed_size));
	const auto decompressed_size = duckdb_zstd::ZSTD_decompress(
	    buffer->InternalBuffer(), buffer->AllocSize(), compressed_buffer.get() + sizeof(idx_t), compressed_size);
	(void)decompressed_size;
	D_ASSERT(!duckdb_zstd::ZSTD_isError(decompressed_size));

	D_ASSERT(decompressed_size == buffer->AllocSize());
	return buffer;
}

void TemporaryFileHandle::WriteTemporaryBuffer(FileBuffer &buffer, const idx_t block_index,
                                               AllocatedData &compressed_buffer) const {
	// We group DEFAULT_BLOCK_ALLOC_SIZE blocks into the same file.
	D_ASSERT(buffer.AllocSize() == BufferManager::GetBufferManager(db).GetBlockAllocSize());
	if (identifier.size == TemporaryBufferSize::DEFAULT) {
		buffer.Write(*handle, GetPositionInFile(block_index));
	} else {
		handle->Write(compressed_buffer.get(), TemporaryBufferSizeToSize(identifier.size),
		              GetPositionInFile(block_index));
	}
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
	if (index_manager.RemoveIndex(index, identifier.size)) {
		// the max_index that is currently in use has decreased
		// as a result we can truncate the file
#ifndef WIN32 // this ended up causing issues when sorting
		auto max_index = index_manager.GetMaxIndex();
		auto &fs = FileSystem::GetFileSystem(db);
		fs.Truncate(*handle, NumericCast<int64_t>(GetPositionInFile(max_index + 1)));
#endif
	}
}

idx_t TemporaryFileHandle::GetPositionInFile(const idx_t index) const {
	return index * static_cast<idx_t>(identifier.size);
}

//===--------------------------------------------------------------------===//
// TemporaryFileMap
//===--------------------------------------------------------------------===//
TemporaryFileMap::TemporaryFileMap(TemporaryFileManager &manager_p) : manager(manager_p) {
}

void TemporaryFileMap::Clear() {
	files.clear();
}

TemporaryFileMap::temporary_file_map_t &TemporaryFileMap::GetMapForSize(const TemporaryBufferSize size) {
	D_ASSERT(TemporaryBufferSizeIsValid(size));
	return files[size];
}

optional_ptr<TemporaryFileHandle> TemporaryFileMap::GetFile(const TemporaryFileIdentifier &identifier) {
	D_ASSERT(identifier.IsValid());
	auto &map = GetMapForSize(identifier.size);
	const auto it = map.find(identifier.file_index.GetIndex());
	return it == map.end() ? nullptr : it->second.get();
}

TemporaryFileHandle &TemporaryFileMap::CreateFile(const TemporaryFileIdentifier &identifier) {
	D_ASSERT(identifier.IsValid());
	D_ASSERT(!GetFile(identifier));
	auto &map = GetMapForSize(identifier.size);
	const auto res =
	    map.emplace(identifier.file_index.GetIndex(), make_uniq<TemporaryFileHandle>(manager, identifier, map.size()));
	D_ASSERT(res.second);
	return *res.first->second;
}

void TemporaryFileMap::EraseFile(const TemporaryFileIdentifier &identifier) {
	D_ASSERT(identifier.IsValid());
	D_ASSERT(GetFile(identifier));
	GetMapForSize(identifier.size).erase(identifier.file_index.GetIndex());
}

//===--------------------------------------------------------------------===//
// TemporaryFileCompressionLevel/TemporaryFileCompressionAdaptivity
//===--------------------------------------------------------------------===//
TemporaryFileCompressionAdaptivity::TemporaryFileCompressionAdaptivity() : last_uncompressed_write_ns(INITIAL_NS) {
	for (idx_t i = 0; i < LEVELS; i++) {
		last_compressed_writes_ns[i] = INITIAL_NS;
	}
}

int64_t TemporaryFileCompressionAdaptivity::GetCurrentTimeNanos() {
	return duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

TemporaryCompressionLevel TemporaryFileCompressionAdaptivity::IndexToLevel(const idx_t index) {
	return static_cast<TemporaryCompressionLevel>(NumericCast<int>(index) * 2 - 5);
}

idx_t TemporaryFileCompressionAdaptivity::LevelToIndex(const TemporaryCompressionLevel level) {
	return NumericCast<idx_t>((static_cast<int>(level) + 5) / 2);
}

TemporaryCompressionLevel TemporaryFileCompressionAdaptivity::MinimumCompressionLevel() {
	return IndexToLevel(0);
}

TemporaryCompressionLevel TemporaryFileCompressionAdaptivity::MaximumCompressionLevel() {
	return IndexToLevel(LEVELS - 1);
}

TemporaryCompressionLevel TemporaryFileCompressionAdaptivity::GetCompressionLevel() {
	idx_t min_compression_idx = 0;
	TemporaryCompressionLevel level;

	double ratio;
	bool should_compress;

	bool should_deviate;
	bool deviate_uncompressed;
	{
		lock_guard<mutex> guard(random_engine.lock);

		auto min_compressed_time = last_compressed_writes_ns[min_compression_idx];
		for (idx_t compression_idx = 1; compression_idx < LEVELS; compression_idx++) {
			const auto time = last_compressed_writes_ns[compression_idx];
			if (time < min_compressed_time) {
				min_compression_idx = compression_idx;
				min_compressed_time = time;
			}
		}
		level = IndexToLevel(min_compression_idx);

		ratio = static_cast<double>(min_compressed_time) / static_cast<double>(last_uncompressed_write_ns);
		should_compress = ratio < DURATION_RATIO_THRESHOLD;

		should_deviate = random_engine.NextRandom() < COMPRESSION_DEVIATION;
		deviate_uncompressed = random_engine.NextRandom() < 0.5; // Coin flip to deviate with just uncompressed
	}

	TemporaryCompressionLevel result;
	if (!should_deviate) {
		result = should_compress ? level : TemporaryCompressionLevel::UNCOMPRESSED; // Don't deviate
	} else if (!should_compress) {
		result = MinimumCompressionLevel(); // Deviate from uncompressed -> go to fastest level
	} else if (deviate_uncompressed) {
		result = TemporaryCompressionLevel::UNCOMPRESSED;
	} else if (level == MaximumCompressionLevel()) {
		result = IndexToLevel(min_compression_idx - 1); // At highest level, go down one
	} else if (ratio < 1.0) { // Compressed writes are faster, try increasing the compression level
		result = IndexToLevel(min_compression_idx + 1);
	} else { // Compressed writes are slower, try decreasing the compression level
		result = level == MinimumCompressionLevel()
		             ? TemporaryCompressionLevel::UNCOMPRESSED // Already lowest level, go to uncompressed
		             : IndexToLevel(min_compression_idx - 1);
	}
	return result;
}

void TemporaryFileCompressionAdaptivity::Update(const TemporaryCompressionLevel level, const int64_t time_before_ns) {
	const auto duration = GetCurrentTimeNanos() - time_before_ns;
	auto &last_write_ns = level == TemporaryCompressionLevel::UNCOMPRESSED
	                          ? last_uncompressed_write_ns
	                          : last_compressed_writes_ns[LevelToIndex(level)];
	lock_guard<mutex> guard(random_engine.lock);
	last_write_ns = (last_write_ns * (WEIGHT - 1) + duration) / WEIGHT;
}

//===--------------------------------------------------------------------===//
// TemporaryFileManager
//===--------------------------------------------------------------------===//
TemporaryFileManager::TemporaryFileManager(DatabaseInstance &db, const string &temp_directory_p)
    : db(db), temp_directory(temp_directory_p), files(*this), size_on_disk(0), max_swap_space(0) {
}

TemporaryFileManager::~TemporaryFileManager() {
	files.Clear();
}

TemporaryFileManager::TemporaryFileManagerLock::TemporaryFileManagerLock(mutex &mutex) : lock(mutex) {
}

void TemporaryFileManager::WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer) {
	// We group DEFAULT_BLOCK_ALLOC_SIZE blocks into the same file.
	D_ASSERT(buffer.AllocSize() == BufferManager::GetBufferManager(db).GetBlockAllocSize());

	const auto adaptivity_idx = TaskScheduler::GetEstimatedCPUId() % COMPRESSION_ADAPTIVITIES;
	auto &compression_adaptivity = compression_adaptivities[adaptivity_idx];

	const auto time_before_ns = TemporaryFileCompressionAdaptivity::GetCurrentTimeNanos();
	AllocatedData compressed_buffer;
	const auto compression_result = CompressBuffer(compression_adaptivity, buffer, compressed_buffer);

	TemporaryFileIndex index;
	optional_ptr<TemporaryFileHandle> handle;
	{
		TemporaryFileManagerLock lock(manager_lock);
		// first check if we can write to an open existing file
		for (auto &entry : files.GetMapForSize(compression_result.size)) {
			auto &temp_file = entry.second;
			index = temp_file->TryGetBlockIndex();
			if (index.IsValid()) {
				handle = entry.second.get();
				break;
			}
		}
		if (!handle) {
			// no existing handle to write to; we need to create & open a new file
			auto &size = compression_result.size;
			const TemporaryFileIdentifier identifier(size, index_managers[size].GetNewBlockIndex(size));
			auto &new_file = files.CreateFile(identifier);
			index = new_file.TryGetBlockIndex();
			handle = &new_file;
		}
		D_ASSERT(used_blocks.find(block_id) == used_blocks.end());
		used_blocks[block_id] = index;
	}
	D_ASSERT(handle);
	D_ASSERT(index.IsValid());

	handle->WriteTemporaryBuffer(buffer, index.block_index.GetIndex(), compressed_buffer);

	compression_adaptivity.Update(compression_result.level, time_before_ns);
}

TemporaryFileManager::CompressionResult
TemporaryFileManager::CompressBuffer(TemporaryFileCompressionAdaptivity &compression_adaptivity, FileBuffer &buffer,
                                     AllocatedData &compressed_buffer) {
	if (buffer.AllocSize() <= TemporaryBufferSizeToSize(MinimumCompressedTemporaryBufferSize())) {
		// Buffer size is less or equal to the minimum compressed size - no point compressing
		return {TemporaryBufferSize::DEFAULT, TemporaryCompressionLevel::UNCOMPRESSED};
	}

	const auto level = compression_adaptivity.GetCompressionLevel();
	if (level == TemporaryCompressionLevel::UNCOMPRESSED) {
		return {TemporaryBufferSize::DEFAULT, TemporaryCompressionLevel::UNCOMPRESSED};
	}

	const auto compression_level = static_cast<int>(level);
	D_ASSERT(compression_level >= duckdb_zstd::ZSTD_minCLevel() && compression_level <= duckdb_zstd::ZSTD_maxCLevel());
	const auto zstd_bound = duckdb_zstd::ZSTD_compressBound(buffer.AllocSize());
	compressed_buffer = Allocator::Get(db).Allocate(sizeof(idx_t) + zstd_bound);
	const auto zstd_size = duckdb_zstd::ZSTD_compress(compressed_buffer.get() + sizeof(idx_t), zstd_bound,
	                                                  buffer.InternalBuffer(), buffer.AllocSize(), compression_level);
	D_ASSERT(!duckdb_zstd::ZSTD_isError(zstd_size));
	Store<idx_t>(zstd_size, compressed_buffer.get());
	const auto compressed_size = sizeof(idx_t) + zstd_size;

	if (compressed_size > TemporaryBufferSizeToSize(MaximumCompressedTemporaryBufferSize())) {
		return {TemporaryBufferSize::DEFAULT, level}; // Use default size if compression ratio is bad
	}

	return {RoundUpSizeToTemporaryBufferSize(compressed_size), level};
}

bool TemporaryFileManager::HasTemporaryBuffer(block_id_t block_id) {
	lock_guard<mutex> lock(manager_lock);
	return used_blocks.find(block_id) != used_blocks.end();
}

idx_t TemporaryFileManager::GetTotalUsedSpaceInBytes() const {
	return size_on_disk.load();
}

optional_idx TemporaryFileManager::GetMaxSwapSpace() const {
	return max_swap_space;
}

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

void TemporaryFileManager::SetMaxSwapSpace(optional_idx limit) {
	idx_t new_limit;
	if (limit.IsValid()) {
		new_limit = limit.GetIndex();
	} else {
		new_limit = GetDefaultMax(temp_directory);
	}

	auto current_size_on_disk = GetTotalUsedSpaceInBytes();
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
	auto current_size_on_disk = GetTotalUsedSpaceInBytes();
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
	optional_ptr<TemporaryFileHandle> handle;
	{
		TemporaryFileManagerLock lock(manager_lock);
		index = GetTempBlockIndex(lock, id);
		handle = GetFileHandle(lock, index.identifier);
	}

	auto buffer = handle->ReadTemporaryBuffer(index.block_index.GetIndex(), std::move(reusable_buffer));
	{
		// remove the block (and potentially erase the temp file)
		TemporaryFileManagerLock lock(manager_lock);
		EraseUsedBlock(lock, id, *handle, index);
	}
	return buffer;
}

void TemporaryFileManager::DeleteTemporaryBuffer(block_id_t id) {
	TemporaryFileManagerLock lock(manager_lock);
	auto index = GetTempBlockIndex(lock, id);
	auto handle = GetFileHandle(lock, index.identifier);
	EraseUsedBlock(lock, id, *handle, index);
}

vector<TemporaryFileInformation> TemporaryFileManager::GetTemporaryFiles() {
	lock_guard<mutex> lock(manager_lock);
	vector<TemporaryFileInformation> result;
	for (auto &size : TemporaryBufferSizes()) {
		for (const auto &file : files.GetMapForSize(size)) {
			result.push_back(file.second->GetTemporaryFile());
		}
	}
	return result;
}

void TemporaryFileManager::EraseUsedBlock(TemporaryFileManagerLock &lock, block_id_t id, TemporaryFileHandle &handle,
                                          TemporaryFileIndex index) {
	auto entry = used_blocks.find(id);
	if (entry == used_blocks.end()) {
		throw InternalException("EraseUsedBlock - Block %llu not found in used blocks", id);
	}
	used_blocks.erase(entry);
	handle.EraseBlockIndex(NumericCast<block_id_t>(index.block_index.GetIndex()));
	if (handle.DeleteIfEmpty()) {
		EraseFileHandle(lock, index.identifier);
	}
}

string TemporaryFileManager::CreateTemporaryFileName(const TemporaryFileIdentifier &identifier) const {
	return FileSystem::GetFileSystem(db).JoinPath(
	    temp_directory, StringUtil::Format("duckdb_temp_storage_%s-%llu.tmp", EnumUtil::ToString(identifier.size),
	                                       identifier.file_index.GetIndex()));
}

optional_ptr<TemporaryFileHandle> TemporaryFileManager::GetFileHandle(TemporaryFileManagerLock &,
                                                                      const TemporaryFileIdentifier &identifier) {
	D_ASSERT(identifier.IsValid());
	return files.GetFile(identifier);
}

TemporaryFileIndex TemporaryFileManager::GetTempBlockIndex(TemporaryFileManagerLock &, block_id_t id) {
	D_ASSERT(used_blocks.find(id) != used_blocks.end());
	return used_blocks[id];
}

void TemporaryFileManager::EraseFileHandle(TemporaryFileManagerLock &, const TemporaryFileIdentifier &identifier) {
	D_ASSERT(identifier.IsValid());
	files.EraseFile(identifier);
	index_managers[identifier.size].RemoveIndex(identifier.file_index.GetIndex(), identifier.size);
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

TemporaryFileManager &TemporaryDirectoryHandle::GetTempFile() const {
	return *temp_file;
}

} // namespace duckdb
