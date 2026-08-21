#include "duckdb/storage/temporary_file_manager.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer/temporary_file_information.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "duckdb/storage/storage_options.hpp"
#include "zstd.h"

namespace duckdb {

static constexpr const char *TEMPORARY_OWNER_FILE_PREFIX = "duckdb_temp_owner_v1_";
static constexpr const char *TEMPORARY_OWNER_FILE_SUFFIX = ".lock";
static constexpr const char *TEMPORARY_FILE_PREFIX = "duckdb_temp_v1_";
static constexpr const char *TEMPORARY_OWNER_MAGIC = "DUCKDB_TEMP_OWNER";
static constexpr idx_t TEMPORARY_OWNER_VERSION = 1;
static constexpr idx_t TEMPORARY_OWNER_ID_LENGTH = 36;
static constexpr idx_t TEMPORARY_OWNER_UUID_VERSION_INDEX = 14;
static constexpr idx_t TEMPORARY_OWNER_UUID_VARIANT_INDEX = 19;
static constexpr idx_t TEMPORARY_OWNER_CREATE_ATTEMPTS = 3;

struct ActiveTemporaryOwnerRegistry {
	// POSIX fcntl locks are process-scoped, so they cannot distinguish DuckDB instances in the same process.
	mutex lock;
	unordered_set<string> owners;
};

static ActiveTemporaryOwnerRegistry &GetActiveTemporaryOwnerRegistry() {
	static ActiveTemporaryOwnerRegistry registry;
	return registry;
}

static bool RegisterActiveTemporaryOwner(const string &owner_id) {
	auto &registry = GetActiveTemporaryOwnerRegistry();
	lock_guard<mutex> guard(registry.lock);
	return registry.owners.insert(owner_id).second;
}

static void UnregisterActiveTemporaryOwner(const string &owner_id) {
	auto &registry = GetActiveTemporaryOwnerRegistry();
	lock_guard<mutex> guard(registry.lock);
	registry.owners.erase(owner_id);
}

static bool IsActiveTemporaryOwner(const string &owner_id) {
	auto &registry = GetActiveTemporaryOwnerRegistry();
	lock_guard<mutex> guard(registry.lock);
	return registry.owners.find(owner_id) != registry.owners.end();
}

static string TemporaryOwnerFileName(const string &owner_id) {
	return string(TEMPORARY_OWNER_FILE_PREFIX) + owner_id + TEMPORARY_OWNER_FILE_SUFFIX;
}

static string TemporaryOwnerManifest(const string &owner_id) {
	return StringUtil::Format("%s\n%llu\n%s\n", TEMPORARY_OWNER_MAGIC, TEMPORARY_OWNER_VERSION, owner_id);
}

static string OwnedTemporaryFilePrefix(const string &owner_id) {
	return string(TEMPORARY_FILE_PREFIX) + owner_id + "_";
}

static bool IsTemporaryOwnerId(const string &candidate) {
	if (candidate.size() != TEMPORARY_OWNER_ID_LENGTH) {
		return false;
	}
	hugeint_t parsed_uuid;
	return UUID::FromString(candidate, parsed_uuid, true) && UUID::ToString(parsed_uuid) == candidate &&
	       candidate[TEMPORARY_OWNER_UUID_VERSION_INDEX] == '4' &&
	       (candidate[TEMPORARY_OWNER_UUID_VARIANT_INDEX] == '8' ||
	        candidate[TEMPORARY_OWNER_UUID_VARIANT_INDEX] == '9' ||
	        candidate[TEMPORARY_OWNER_UUID_VARIANT_INDEX] == 'a' ||
	        candidate[TEMPORARY_OWNER_UUID_VARIANT_INDEX] == 'b');
}

static bool TryParseTemporaryOwnerFile(const string &file_name, string &owner_id) {
	if (!StringUtil::StartsWith(file_name, TEMPORARY_OWNER_FILE_PREFIX) ||
	    !StringUtil::EndsWith(file_name, TEMPORARY_OWNER_FILE_SUFFIX)) {
		return false;
	}
	const auto prefix_size = strlen(TEMPORARY_OWNER_FILE_PREFIX);
	const auto suffix_size = strlen(TEMPORARY_OWNER_FILE_SUFFIX);
	if (file_name.size() <= prefix_size + suffix_size) {
		return false;
	}
	auto candidate = file_name.substr(prefix_size, file_name.size() - prefix_size - suffix_size);
	if (!IsTemporaryOwnerId(candidate)) {
		return false;
	}
	owner_id = std::move(candidate);
	return true;
}

static bool IsUnsignedNumber(const string &value) {
	if (value.empty()) {
		return false;
	}
	for (const auto ch : value) {
		if (ch < '0' || ch > '9') {
			return false;
		}
	}
	return true;
}

static bool IsTemporaryStorageSize(const string &value) {
	return value == "S32K" || value == "S64K" || value == "S96K" || value == "S128K" || value == "S160K" ||
	       value == "S192K" || value == "S224K" || value == "DEFAULT";
}

static bool TryParseOwnedTemporaryFile(const string &file_name, string &owner_id) {
	if (!StringUtil::StartsWith(file_name, TEMPORARY_FILE_PREFIX)) {
		return false;
	}
	const auto prefix_size = strlen(TEMPORARY_FILE_PREFIX);
	if (file_name.size() <= prefix_size + TEMPORARY_OWNER_ID_LENGTH ||
	    file_name[prefix_size + TEMPORARY_OWNER_ID_LENGTH] != '_') {
		return false;
	}
	auto candidate = file_name.substr(prefix_size, TEMPORARY_OWNER_ID_LENGTH);
	if (!IsTemporaryOwnerId(candidate)) {
		return false;
	}
	const auto remainder = file_name.substr(prefix_size + TEMPORARY_OWNER_ID_LENGTH + 1);
	if (StringUtil::StartsWith(remainder, "block-") && StringUtil::EndsWith(remainder, ".block")) {
		const auto block_id =
		    remainder.substr(strlen("block-"), remainder.size() - strlen("block-") - strlen(".block"));
		if (!IsUnsignedNumber(block_id)) {
			return false;
		}
		owner_id = std::move(candidate);
		return true;
	}
	if (!StringUtil::StartsWith(remainder, "storage_") || !StringUtil::EndsWith(remainder, ".tmp")) {
		return false;
	}
	const auto storage_id =
	    remainder.substr(strlen("storage_"), remainder.size() - strlen("storage_") - strlen(".tmp"));
	const auto separator = storage_id.rfind('-');
	if (separator == string::npos) {
		return false;
	}
	if (!IsTemporaryStorageSize(storage_id.substr(0, separator)) ||
	    !IsUnsignedNumber(storage_id.substr(separator + 1))) {
		return false;
	}
	owner_id = std::move(candidate);
	return true;
}

static bool IsOwnedTemporaryFile(const string &file_name, const string &owner_id) {
	string parsed_owner_id;
	return TryParseOwnedTemporaryFile(file_name, parsed_owner_id) && parsed_owner_id == owner_id;
}

static bool TemporaryOwnerManifestMatches(FileHandle &handle, const string &owner_id) {
	auto expected = TemporaryOwnerManifest(owner_id);
	if (handle.GetFileSize() != expected.size()) {
		return false;
	}
	string actual(expected.size(), '\0');
	handle.Read(QueryContext(), data_ptr_cast(&actual[0]), actual.size(), 0);
	return actual == expected;
}

struct LockedTemporaryOwner {
	string owner_file_path;
	unique_ptr<FileHandle> owner_file_handle;
	vector<string> files;
};

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

TemporaryFileIdentifier::TemporaryFileIdentifier(DatabaseInstance &db, TemporaryBufferSize size_p, idx_t file_index_p,
                                                 bool encrypted_p)
    : size(size_p), file_index(file_index_p), encrypted(encrypted_p) {
	if (encrypted) {
		// generate a random encryption key ID and corresponding key
		EncryptionEngine::AddTempKeyToCache(db);
	}
}

bool TemporaryFileIdentifier::IsValid() const {
	return size != TemporaryBufferSize::INVALID && file_index.IsValid();
}

TemporaryFileIndex::TemporaryFileIndex() {
}

TemporaryFileIndex::TemporaryFileIndex(TemporaryFileIdentifier identifier_p, idx_t block_index_p,
                                       idx_t block_header_size_p)
    : identifier(identifier_p), block_index(block_index_p), block_header_size(block_header_size_p) {
}

bool TemporaryFileIndex::IsValid() const {
	return identifier.IsValid() && block_index.IsValid() && block_header_size.IsValid();
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

idx_t BlockIndexManager::GetUsedBlockCount() const {
	return indexes_in_use.size();
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

TemporaryFileHandle::~TemporaryFileHandle() {
}

TemporaryFileHandle::TemporaryFileLock::TemporaryFileLock(mutex &mutex) : lock(mutex) {
}

TemporaryFileIndex TemporaryFileHandle::TryGetBlockIndex(idx_t block_header_size) {
	TemporaryFileLock lock(file_lock);
	if (index_manager.GetMaxIndex() >= max_allowed_index && !index_manager.HasFreeBlocks()) {
		// file is at capacity
		return TemporaryFileIndex();
	}
	// open the file handle if it does not yet exist
	CreateFileIfNotExists(lock);
	// fetch a new block index to write to
	auto block_index = index_manager.GetNewBlockIndex(identifier.size);
	return TemporaryFileIndex(identifier, block_index, block_header_size);
}

unique_ptr<FileBuffer> TemporaryFileHandle::ReadTemporaryBuffer(QueryContext context,
                                                                const TemporaryFileIndex &index_in_file,
                                                                unique_ptr<FileBuffer> reusable_buffer) const {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto block_index = index_in_file.block_index.GetIndex();
	auto block_header_size = index_in_file.block_header_size.GetIndex();

	auto buffer = buffer_manager.ConstructManagedBuffer(buffer_manager.GetBlockAllocSize() - block_header_size,
	                                                    block_header_size, std::move(reusable_buffer));
	AllocatedData compressed_buffer;
	data_ptr_t read_buffer;
	idx_t read_size;
	bool is_uncompressed = identifier.size == TemporaryBufferSize::DEFAULT;
	if (is_uncompressed) {
		// not compressed - read directly into buffer
		read_buffer = buffer->InternalBuffer();
		read_size = buffer->AllocSize();
	} else {
		// compressed - read into separate compression buffer first
		compressed_buffer = Allocator::Get(db).Allocate(TemporaryBufferSizeToSize(identifier.size));
		read_buffer = compressed_buffer.get();
		read_size = compressed_buffer.GetSize();
	}

	idx_t read_position = GetPositionInFile(block_index);
	if (IsEncrypted()) {
		uint8_t encryption_metadata[DEFAULT_ENCRYPTED_BUFFER_HEADER_SIZE];
		//! Read nonce and tag.
		handle->Read(context, encryption_metadata, DEFAULT_ENCRYPTED_BUFFER_HEADER_SIZE, read_position);
		//! Read the encrypted compressed buffer.
		handle->Read(context, read_buffer, read_size, read_position + DEFAULT_ENCRYPTED_BUFFER_HEADER_SIZE);
		//! Decrypt the compressed buffer.
		EncryptionEngine::DecryptTemporaryBuffer(db, read_buffer, read_size, encryption_metadata);
	} else {
		handle->Read(context, read_buffer, read_size, read_position);
	}

	if (is_uncompressed) {
		return buffer;
	}

	// Decompress into buffer
	const auto compressed_size = Load<idx_t>(compressed_buffer.get());
	D_ASSERT(!duckdb_zstd::ZSTD_isError(compressed_size));
	const auto decompressed_size = duckdb_zstd::ZSTD_decompress(
	    buffer->InternalBuffer(), buffer->AllocSize(), compressed_buffer.get() + sizeof(idx_t), compressed_size);
	(void)decompressed_size;
	D_ASSERT(!duckdb_zstd::ZSTD_isError(decompressed_size));

	D_ASSERT(decompressed_size == buffer->AllocSize());
	return buffer;
}

void TemporaryFileHandle::WriteTemporaryBuffer(QueryContext context, FileBuffer &buffer, const idx_t block_index,
                                               AllocatedData &compressed_buffer) const {
	// We group DEFAULT_BLOCK_ALLOC_SIZE blocks into the same file.
	D_ASSERT(buffer.AllocSize() == BufferManager::GetBufferManager(db).GetBlockAllocSize());
	bool is_uncompressed = identifier.size == TemporaryBufferSize::DEFAULT;
	data_ptr_t write_buffer;
	idx_t write_size;
	if (is_uncompressed) {
		// write the uncompressed buffer to the file directly
		write_buffer = buffer.InternalBuffer();
		write_size = buffer.AllocSize();
	} else {
		// write the compressed buffer to the file
		write_buffer = compressed_buffer.get();
		write_size = TemporaryBufferSizeToSize(identifier.size);
	}
	idx_t write_position = GetPositionInFile(block_index);
	if (IsEncrypted()) {
		uint8_t encryption_metadata[DEFAULT_ENCRYPTED_BUFFER_HEADER_SIZE];
		EncryptionEngine::EncryptTemporaryBuffer(db, write_buffer, write_size, encryption_metadata);

		handle->Write(context, encryption_metadata, DEFAULT_ENCRYPTED_BUFFER_HEADER_SIZE, write_position);
		handle->Write(context, write_buffer, write_size, write_position + DEFAULT_ENCRYPTED_BUFFER_HEADER_SIZE);
	} else {
		// write file directly
		handle->Write(context, write_buffer, write_size, write_position);
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

bool TemporaryFileHandle::IsEncrypted() const {
	return identifier.encrypted;
}

TemporaryFileInformation TemporaryFileHandle::GetTemporaryFile() {
	TemporaryFileLock lock(file_lock);
	TemporaryFileInformation info;
	info.path = path;
	info.size = GetPositionInFile(index_manager.GetUsedBlockCount());
	return info;
}

void TemporaryFileHandle::CreateFileIfNotExists(TemporaryFileLock &) {
	if (handle) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(db);
	auto open_flags = FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;
	// Temp files have no per-file IO_MODE; they follow the global default_io_mode setting.
	if (Settings::Get<DefaultIoModeSetting>(db) == FileIOMode::DIRECT_IO) {
		open_flags |= FileFlags::FILE_FLAGS_DIRECT_IO;
	}
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
	idx_t metadata_size = 0;
	if (IsEncrypted()) {
		// if the file is encrypted, we need to account for the header size
		metadata_size = DEFAULT_ENCRYPTED_BUFFER_HEADER_SIZE;
	}
	return index * (static_cast<idx_t>(identifier.size) + metadata_size);
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

void TemporaryFileCompressionAdaptivity::Update(const TemporaryCompressionLevel level, const TimePoint &time_before) {
	const auto duration = time_before.ElapsedNanos();
	auto &last_write_ns = level == TemporaryCompressionLevel::UNCOMPRESSED
	                          ? last_uncompressed_write_ns
	                          : last_compressed_writes_ns[LevelToIndex(level)];
	lock_guard<mutex> guard(random_engine.lock);
	last_write_ns = (last_write_ns * (WEIGHT - 1) + duration) / WEIGHT;
}

//===--------------------------------------------------------------------===//
// TemporaryFileManager
//===--------------------------------------------------------------------===//
TemporaryFileManager::TemporaryFileManager(DatabaseInstance &db, const string &temp_directory_p, string instance_id_p,
                                           atomic<idx_t> &size_on_disk_p)
    : db(db), temp_directory(temp_directory_p), instance_id(std::move(instance_id_p)), files(*this),
      size_on_disk(size_on_disk_p), max_swap_space(0) {
}

TemporaryFileManager::~TemporaryFileManager() {
	files.Clear();
}

TemporaryFileManager::TemporaryFileManagerLock::TemporaryFileManagerLock(mutex &mutex) : lock(mutex) {
}

idx_t TemporaryFileManager::WriteTemporaryBuffer(QueryContext context, block_id_t block_id, FileBuffer &buffer) {
	// We group DEFAULT_BLOCK_ALLOC_SIZE blocks into the same file.
	D_ASSERT(buffer.AllocSize() == BufferManager::GetBufferManager(db).GetBlockAllocSize());

	auto header_size = buffer.GetHeaderSize();
	const auto adaptivity_idx = TaskScheduler::GetEstimatedCPUId() % COMPRESSION_ADAPTIVITIES;
	auto &compression_adaptivity = compression_adaptivities[adaptivity_idx];

	const auto time_before = TimePoint::Tick();
	AllocatedData compressed_buffer;
	const auto compression_result = CompressBuffer(compression_adaptivity, buffer, compressed_buffer);

	TemporaryFileIndex index;
	optional_ptr<TemporaryFileHandle> handle;
	{
		TemporaryFileManagerLock lock(manager_lock);
		// first check if we can write to an open existing file
		for (auto &entry : files.GetMapForSize(compression_result.size)) {
			auto &temp_file = entry.second;
			index = temp_file->TryGetBlockIndex(header_size);
			if (index.IsValid()) {
				handle = entry.second.get();
				break;
			}
		}
		if (!handle) {
			// no existing handle to write to; we need to create & open a new file
			auto &size = compression_result.size;
			const TemporaryFileIdentifier identifier(db, size, index_managers[size].GetNewBlockIndex(size),
			                                         IsEncrypted());
			auto &new_file = files.CreateFile(identifier);
			index = new_file.TryGetBlockIndex(header_size);
			handle = &new_file;
		}
		D_ASSERT(used_blocks.find(block_id) == used_blocks.end());
		used_blocks[block_id] = index;
	}
	D_ASSERT(handle);
	D_ASSERT(index.IsValid());

	handle->WriteTemporaryBuffer(context, buffer, index.block_index.GetIndex(), compressed_buffer);

	compression_adaptivity.Update(compression_result.level, time_before);
	return static_cast<idx_t>(compression_result.size);
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
	return size_on_disk.load(std::memory_order_relaxed);
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

bool TemporaryFileManager::IsEncrypted() const {
	return Settings::Get<TempFileEncryptionSetting>(db);
}

unique_ptr<FileBuffer> TemporaryFileManager::ReadTemporaryBuffer(QueryContext context, block_id_t id,
                                                                 unique_ptr<FileBuffer> reusable_buffer,
                                                                 idx_t *eviction_size) {
	TemporaryFileIndex index;
	optional_ptr<TemporaryFileHandle> handle;
	{
		TemporaryFileManagerLock lock(manager_lock);
		index = GetTempBlockIndex(lock, id);
		handle = GetFileHandle(lock, index.identifier);
	}

	// If eviction size requested, set it to the size of the block (compressed size if applicable).
	if (eviction_size) {
		*eviction_size = NumericCast<idx_t>(index.identifier.size);
	}

	// before the reusable buffer is given,
	auto buffer = handle->ReadTemporaryBuffer(context, index, std::move(reusable_buffer));
	{
		// remove the block (and potentially erase the temp file)
		TemporaryFileManagerLock lock(manager_lock);
		EraseUsedBlock(lock, id, *handle, index);
	}
	return buffer;
}

idx_t TemporaryFileManager::DeleteTemporaryBuffer(block_id_t id) {
	TemporaryFileManagerLock lock(manager_lock);
	auto index = GetTempBlockIndex(lock, id);
	auto handle = GetFileHandle(lock, index.identifier);
	EraseUsedBlock(lock, id, *handle, index);
	return static_cast<idx_t>(index.identifier.size);
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
	    temp_directory,
	    StringUtil::Format("%sstorage_%s-%llu.tmp", OwnedTemporaryFilePrefix(instance_id),
	                       EnumUtil::ToString(identifier.size), identifier.file_index.GetIndex()));
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
TemporaryDirectoryHandle::TemporaryDirectoryHandle(DatabaseInstance &db, string path_p, atomic<idx_t> &size_on_disk,
                                                   optional_idx max_swap_space)
    : db(db), temp_directory(std::move(path_p)) {
	auto &fs = FileSystem::GetFileSystem(db);
	D_ASSERT(!temp_directory.empty());
	if (!fs.DirectoryExists(temp_directory)) {
		fs.CreateDirectory(temp_directory);
	}
	try {
		bool cleanup_orphaned_files = true;
		try {
			InitializeOwnerFile(fs);
		} catch (...) {
			// Establishing ownership is only required for orphan cleanup. Keep spilling in an unregistered UUID
			// namespace if the file system cannot create, lock, write or sync the owner file.
			ReleaseOwnerFile(fs, true);
			instance_id = UUID::ToString(UUID::GenerateRandomUUID());
			cleanup_orphaned_files = false;
		}
		if (cleanup_orphaned_files) {
			CleanupOrphanedFiles(fs);
		}
		temp_file = make_uniq<TemporaryFileManager>(db, temp_directory, instance_id, size_on_disk);
		temp_file->SetMaxSwapSpace(max_swap_space);
	} catch (...) {
		ReleaseOwnerFile(fs, true);
		throw;
	}
}

TemporaryDirectoryHandle::~TemporaryDirectoryHandle() {
	temp_file.reset();
	auto &fs = FileSystem::GetFileSystem(db);
	bool remove_owner_file = true;
	try {
		RemoveOwnedTemporaryFiles(fs, instance_id);
	} catch (...) {
		remove_owner_file = false;
	}
	ReleaseOwnerFile(fs, remove_owner_file);
}

void TemporaryDirectoryHandle::InitializeOwnerFile(FileSystem &fs) {
	const auto flags = FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
	                   FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE | FileFlags::FILE_FLAGS_NULL_IF_EXISTS |
	                   FileFlags::FILE_FLAGS_DISABLE_LOGGING | FileLockType::WRITE_LOCK;
	for (idx_t attempt = 0; attempt < TEMPORARY_OWNER_CREATE_ATTEMPTS; attempt++) {
		auto candidate = UUID::ToString(UUID::GenerateRandomUUID());
		if (!RegisterActiveTemporaryOwner(candidate)) {
			continue;
		}
		string candidate_path;
		unique_ptr<FileHandle> candidate_handle;
		try {
			candidate_path = fs.JoinPath(temp_directory, TemporaryOwnerFileName(candidate));
			candidate_handle = fs.OpenFile(candidate_path, flags);
		} catch (...) {
			UnregisterActiveTemporaryOwner(candidate);
			if (!candidate_path.empty()) {
				fs.TryRemoveFile(candidate_path);
			}
			throw;
		}
		if (!candidate_handle) {
			UnregisterActiveTemporaryOwner(candidate);
			continue;
		}

		instance_id = std::move(candidate);
		owner_file_path = std::move(candidate_path);
		owner_file_handle = std::move(candidate_handle);
		owner_registered = true;
		auto manifest = TemporaryOwnerManifest(instance_id);
		owner_file_handle->Write(QueryContext(), &manifest[0], manifest.size(), 0);
		fs.FileSync(*owner_file_handle);
		return;
	}
	throw IOException("Failed to create a unique owner file in temporary directory \"%s\"", temp_directory);
}

void TemporaryDirectoryHandle::CleanupOrphanedFiles(FileSystem &fs) {
	vector<pair<string, string>> owner_files;
	try {
		fs.ListFiles(temp_directory, [&](const string &file_name, bool is_directory) {
			if (is_directory) {
				return;
			}
			string owner_id;
			if (TryParseTemporaryOwnerFile(file_name, owner_id)) {
				owner_files.emplace_back(std::move(file_name), std::move(owner_id));
			}
		});
	} catch (IOException &) {
		return;
	}

	vector<LockedTemporaryOwner> locked_owners;
	unordered_map<string, idx_t> locked_owner_indexes;
	for (const auto &entry : owner_files) {
		const auto &owner_file_name = entry.first;
		const auto &owner_id = entry.second;
		if (IsActiveTemporaryOwner(owner_id)) {
			continue;
		}
		auto path = fs.JoinPath(temp_directory, owner_file_name);
		try {
			auto cleanup_lock = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE |
			                                          FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS |
			                                          FileFlags::FILE_FLAGS_DISABLE_LOGGING |
			                                          FileLockType::WRITE_LOCK);
			if (!cleanup_lock || !TemporaryOwnerManifestMatches(*cleanup_lock, owner_id)) {
				continue;
			}
			locked_owner_indexes.emplace(owner_id, locked_owners.size());
			locked_owners.push_back({std::move(path), std::move(cleanup_lock), {}});
		} catch (IOException &) {
			// A lock failure means the owner is still alive. Other I/O errors leave the owner marker for a later retry.
			continue;
		}
	}
	if (locked_owners.empty()) {
		return;
	}

	try {
		fs.ListFiles(temp_directory, [&](const string &file_name, bool is_directory) {
			if (is_directory) {
				return;
			}
			string owner_id;
			if (!TryParseOwnedTemporaryFile(file_name, owner_id)) {
				return;
			}
			const auto entry = locked_owner_indexes.find(owner_id);
			if (entry == locked_owner_indexes.end()) {
				return;
			}
			locked_owners[entry->second].files.push_back(fs.JoinPath(temp_directory, file_name));
		});
	} catch (IOException &) {
		return;
	}

	for (auto &owner : locked_owners) {
		try {
			fs.RemoveFiles(owner.files);
			fs.TryRemoveFile(owner.owner_file_path);
		} catch (IOException &) {
			// Leave the owner marker behind so a later process can retry cleanup.
			continue;
		}
	}
}

void TemporaryDirectoryHandle::RemoveOwnedTemporaryFiles(FileSystem &fs, const string &owner_id) {
	if (owner_id.empty()) {
		return;
	}
	vector<string> files_to_delete;
	fs.ListFiles(temp_directory, [&](const string &file_name, bool is_directory) {
		if (!is_directory && IsOwnedTemporaryFile(file_name, owner_id)) {
			files_to_delete.push_back(fs.JoinPath(temp_directory, file_name));
		}
	});
	fs.RemoveFiles(files_to_delete);
}

void TemporaryDirectoryHandle::UnregisterOwner() {
	if (!owner_registered) {
		return;
	}
	UnregisterActiveTemporaryOwner(instance_id);
	owner_registered = false;
}

void TemporaryDirectoryHandle::ReleaseOwnerFile(FileSystem &fs, bool remove_owner_file) {
	owner_file_handle.reset();
	UnregisterOwner();
	if (remove_owner_file && !owner_file_path.empty()) {
		fs.TryRemoveFile(owner_file_path);
	}
	owner_file_path.clear();
}

TemporaryFileManager &TemporaryDirectoryHandle::GetTempFile() const {
	D_ASSERT(temp_file);
	return *temp_file;
}

string TemporaryDirectoryHandle::GetTempBlockPath(block_id_t id) const {
	return FileSystem::GetFileSystem(db).JoinPath(
	    temp_directory, StringUtil::Format("%sblock-%llu.block", OwnedTemporaryFilePrefix(instance_id), id));
}

} // namespace duckdb
