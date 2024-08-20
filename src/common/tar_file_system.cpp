#include "duckdb/common/tar_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Tar Metadata Cache
//------------------------------------------------------------------------------
struct TarArchiveFileMetadataCache final : public ObjectCacheEntry {
public:
	TarArchiveFileMetadataCache() : read_time(0), byte_offset(0), byte_size(0) {
	}
	TarArchiveFileMetadataCache(time_t read_time_p, idx_t byte_offset_p, idx_t byte_size_p)
	    : read_time(read_time_p), byte_offset(byte_offset_p), byte_size(byte_size_p) {
	}

	//! Read time (of the archive as a whole)
	time_t read_time;

	//! Byte offset to the file in the tar archive
	idx_t byte_offset;

	//! Byte length of the file in the tar archive
	idx_t byte_size;

public:
	static string ObjectType() {
		return "tar_archive_metadata";
	}

	string GetObjectType() override {
		return ObjectType();
	}
};

static shared_ptr<TarArchiveFileMetadataCache> TryGetCachedArchiveMetadata(optional_ptr<FileOpener> opener,
                                                                           FileHandle &handle, const string &path) {
	// Is this file compressed?
	if (handle.GetFileCompressionType() != FileCompressionType::UNCOMPRESSED) {
		return nullptr;
	}
	// Do we have a client context?
	if (!opener) {
		return nullptr;
	}
	auto context = opener->TryGetClientContext();
	if (!context) {
		return nullptr;
	}
	// Is this archive file already in the cache?
	if (!ObjectCache::ObjectCacheEnabled(*context)) {
		return nullptr;
	}
	auto &cache = ObjectCache::GetObjectCache(*context);
	auto entry = cache.Get<TarArchiveFileMetadataCache>(path);
	if (!entry) {
		return nullptr;
	}
	// Check if the file has been modified since the last read
	if (handle.file_system.GetLastModifiedTime(handle) > entry->read_time) {
		return nullptr;
	}
	return entry;
}

//------------------------------------------------------------------------------
// Tar Block Iterator
//------------------------------------------------------------------------------
struct TarBlockHeader {
public:
	idx_t GetFileSize() const {
		return strtoul(file_size, nullptr, 8);
	}
	string GetFileName() const {
		// Check if USTAR and the filename prefix are set
		if (strncmp(ustar, "ustar", 5) == 0 && filename_prefix[0] != 0) {
			return string(filename_prefix) + "/" + string(file_name);
		}
		return string(file_name);
	}
	bool IsSet() const {
		return file_name[0] != 0;
	}

	char file_name[100] = {};
	char file_mode[8] = {};
	char owner_id[8] = {};
	char group_id[8] = {};
	char file_size[12] = {};
	char last_modification[12] = {};
	char checksum[8] = {};
	char type[1] = {};
	char linked_file_name[100] = {};
	char ustar[6] = {};
	char ustar_version[2] = {};
	char owner_name[32] = {};
	char group_name[32] = {};
	char device_major[8] = {};
	char device_minor[8] = {};
	char filename_prefix[155] = {};
	char padding[12] = {};
};
static_assert(sizeof(TarBlockHeader) == 512, "TarBlockHeader must be 512 bytes");

struct TarBlockEntry {
	unique_ptr<TarBlockHeader> header;
	idx_t file_offset;
	idx_t file_size;
	string file_name;
};

struct TarBlockIteratorHelper;

struct TarBlockIterator {
	TarBlockIterator() : current_block({nullptr, 0, 0, ""}), archive_handle(nullptr), stop(true) {
	}
	explicit TarBlockIterator(FileHandle &archive_handle_p)
	    : current_block({make_uniq<TarBlockHeader>(), 0, 0, ""}), archive_handle(archive_handle_p), stop(false) {
		Next();
	}

	TarBlockIterator &operator++() {
		Next();
		return *this;
	}

	const TarBlockEntry &operator*() const {
		return current_block;
	}

	bool operator!=(const TarBlockIterator &other) const {
		return stop != other.stop;
	}

	static TarBlockIteratorHelper Scan(FileHandle &archive_handle);

private:
	unordered_map<string, string> ParsePaxRecords(const idx_t file_size) {
		auto bytes_remaining = file_size;
		unordered_map<string, string> pax_records;
		vector<char> buffer;
		while (bytes_remaining > 0) {
			buffer.clear();
			while (bytes_remaining > 0) {
				char c;
				if (!archive_handle->Read(&c, 1)) {
					throw IOException("Failed to read pax key-value size byte");
				}
				bytes_remaining--;
				if (c == ' ') {
					break;
				}
				buffer.push_back(c);
			}
			auto record_size = strtoul(buffer.data(), nullptr, 10);
			buffer.clear();
			// Now parse everything up to the equals sign
			while (bytes_remaining > 0) {
				char c;
				if (!archive_handle->Read(&c, 1)) {
					throw IOException("Failed to read pax key-value size byte");
				}
				bytes_remaining--;
				record_size--;
				if (c == '=') {
					break;
				}
				buffer.push_back(c);
			}
			auto key = string(buffer.begin(), buffer.end());
			buffer.clear();
			// Now parse everything up to the record size
			while (bytes_remaining > 0 && record_size > 0) {
				char c;
				if (!archive_handle->Read(&c, 1)) {
					throw IOException("Failed to read pax key-value size byte");
				}
				bytes_remaining--;
				record_size--;
				if (c == '\n') {
					break;
				}
				buffer.push_back(c);
			}
			pax_records[key] = string(buffer.begin(), buffer.end());
		}
		D_ASSERT(bytes_remaining == 0);
		return pax_records;
	}

	void Next() {
		unordered_map<string, string> pax_records;
		while (true) {
			// Move to the next header
			const auto current_file_blocks = (current_block.file_size + 511) / 512;
			archive_handle->Seek(current_block.file_offset + current_file_blocks * 512);

			if (!archive_handle->Read(current_block.header.get(), sizeof(TarBlockHeader))) {
				stop = true;
				return;
			}
			if (current_block.header->file_name[0] == 0) {
				stop = true;
				return;
			}

			current_block.file_size = current_block.header->GetFileSize();
			current_block.file_offset = archive_handle->SeekPosition();

			// Normal file
			if (current_block.header->type[0] == '0' || current_block.header->type[0] == 0) {
				auto pax_path = pax_records.find("path");
				if (pax_path != pax_records.end()) {
					current_block.file_name = pax_path->second;
				} else {
					current_block.file_name = current_block.header->GetFileName();
				}
				return;
			}

			// Pax header
			if (current_block.header->type[0] == 'x') {
				pax_records = ParsePaxRecords(current_block.file_size);
			}
		}
	}

private:
	TarBlockEntry current_block;
	optional_ptr<FileHandle> archive_handle;
	bool stop;
};

struct TarBlockIteratorHelper {
	explicit TarBlockIteratorHelper(FileHandle &archive_handle_p) : archive_handle(archive_handle_p) {
	}
	TarBlockIterator begin() const {
		return TarBlockIterator {archive_handle};
	}
	TarBlockIterator end() const {
		return TarBlockIterator {};
	}

private:
	FileHandle &archive_handle;
};

inline TarBlockIteratorHelper TarBlockIterator::Scan(FileHandle &archive_handle) {
	return TarBlockIteratorHelper {archive_handle};
}

//------------------------------------------------------------------------------
// Tar Utilities
//------------------------------------------------------------------------------

// Split a tar path into the path to the archive and the path within the archive
static pair<string, string> SplitArchivePath(const string &path) {
	const string suffix = ".tar";

	const auto tar_path = std::search(path.begin(), path.end(), suffix.begin(), suffix.end());

	if (tar_path == path.end()) {
		throw IOException("Could not find a '.tar' archive to open in: '%s'", path);
	}

	const auto suffix_path = tar_path + UnsafeNumericCast<int64_t>(suffix.size());
	if (suffix_path == path.end()) {
		return {path, ""};
	}

	if (*suffix_path == '/') {
		// If there is a slash after the last .tar, we need to remove everything after that
		auto archive_path = string(path.begin(), suffix_path);
		auto file_path = string(suffix_path + 1, path.end());
		return {archive_path, file_path};
	}

	// Else, this is not a raw .tar, e.g. .tar.gz or .target
	throw IOException("Could not find a '.tar' archive to open in: '%s'. Note that DuckDB's tar archive filesystem "
	                  "does not support compressed tar archives (such as '.tar.gz' and '.tar.bz2')",
	                  path);
}

//------------------------------------------------------------------------------
// Tar File Handle
//------------------------------------------------------------------------------

void TarFileHandle::Close() {
	inner_handle->Close();
}

//------------------------------------------------------------------------------
// Tar File System
//------------------------------------------------------------------------------

bool TarFileSystem::CanHandleFile(const string &fpath) {
	// TODO: Check that we can seek into the file
	return fpath.size() > 6 && fpath.substr(0, 6) == "tar://";
}

unique_ptr<FileHandle> TarFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                               optional_ptr<FileOpener> opener) {

	// Get the path to the tar file
	const auto paths = SplitArchivePath(path.substr(6));
	const auto &tar_path = paths.first;
	const auto &file_path = paths.second;

	// Now we need to find the file within the tar file and return out file handle
	auto handle = parent_file_system.OpenFile(tar_path, flags, opener);

	// Check if the offset is cached
	const auto cached_entry = TryGetCachedArchiveMetadata(opener, *handle, path);
	if (cached_entry) {
		const auto start_offset = cached_entry->byte_offset;
		const auto end_offset = start_offset + cached_entry->byte_size;

		// Seek to the cached byte offset
		handle->Seek(start_offset);

		// Return a file handle that reads from the cached byte offset
		return make_uniq<TarFileHandle>(*this, path, std::move(handle), start_offset, end_offset);
	}

	// Else, we need to perform a sequential scan through the tar archive to find the file
	for (const auto &block : TarBlockIterator::Scan(*handle)) {
		if (block.file_name == file_path) {
			auto start_offset = block.file_offset;
			auto end_offset = start_offset + block.header->GetFileSize();
			return make_uniq<TarFileHandle>(*this, path, std::move(handle), start_offset, end_offset);
		}
	}

	throw IOException("Failed to find file: %s", file_path);
}

int64_t TarFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &t_handle = handle.Cast<TarFileHandle>();
	// Dont read past the end of the file
	auto position = t_handle.inner_handle->SeekPosition();
	if (position >= t_handle.end_offset) {
		return 0;
	}
	auto remaining_bytes = t_handle.end_offset - position;
	auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining_bytes);
	return t_handle.inner_handle->Read(buffer, to_read);
}

int64_t TarFileSystem::GetFileSize(FileHandle &handle) {
	auto &t_handle = handle.Cast<TarFileHandle>();
	return UnsafeNumericCast<int64_t>(t_handle.end_offset - t_handle.start_offset);
}

void TarFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &t_handle = handle.Cast<TarFileHandle>();
	t_handle.inner_handle->Seek(t_handle.start_offset + location);
}

void TarFileSystem::Reset(FileHandle &handle) {
	auto &t_handle = handle.Cast<TarFileHandle>();
	t_handle.inner_handle->Reset();
	t_handle.inner_handle->Seek(t_handle.start_offset);
}

idx_t TarFileSystem::SeekPosition(FileHandle &handle) {
	auto &t_handle = handle.Cast<TarFileHandle>();
	return t_handle.inner_handle->SeekPosition() - t_handle.start_offset;
}

bool TarFileSystem::CanSeek() {
	return true;
}

time_t TarFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &t_handle = handle.Cast<TarFileHandle>();
	return parent_file_system.GetLastModifiedTime(*t_handle.inner_handle);
}

FileType TarFileSystem::GetFileType(FileHandle &handle) {
	auto &t_handle = handle.Cast<TarFileHandle>();
	return parent_file_system.GetFileType(*t_handle.inner_handle);
}

bool TarFileSystem::OnDiskFile(FileHandle &handle) {
	auto &t_handle = handle.Cast<TarFileHandle>();
	return t_handle.inner_handle->OnDiskFile();
}

vector<string> TarFileSystem::Glob(const string &path, FileOpener *opener) {

	// Remove the "tar://" prefix
	const auto parts = SplitArchivePath(path.substr(6));
	auto &tar_path = parts.first;
	auto &file_path = parts.second;

	if (HasGlob(tar_path)) {
		throw NotImplementedException("Cannot glob multiple tar files");
	}

	if (!HasGlob(file_path)) {
		// No glob pattern in the file path, just return the file path
		return {path};
	}

	auto pattern_parts = StringUtil::Split(file_path, '/');
	for (auto &part : pattern_parts) {
		if (part == "tar:") {
			// We can not glob into nested tar files
			throw NotImplementedException("Globbing into nested tar files is not supported");
		}
	}

	optional_ptr<ObjectCache> cache;
	optional_ptr<ClientContext> context = opener->TryGetClientContext();
	if (context) {
		if (ObjectCache::ObjectCacheEnabled(*context)) {
			cache = ObjectCache::GetObjectCache(*context);
		}
	}

	// Given the path to the tar file, open it
	auto archive_handle = parent_file_system.OpenFile(tar_path, FileFlags::FILE_FLAGS_READ, opener);
	if (!archive_handle) {
		throw IOException("Failed to open file: %s", tar_path);
	}

	auto last_modified = archive_handle->file_system.GetLastModifiedTime(*archive_handle);
	auto is_uncompressed = archive_handle->GetFileCompressionType() == FileCompressionType::UNCOMPRESSED;

	vector<string> result;
	for (auto &entry : TarBlockIterator::Scan(*archive_handle)) {

		auto entry_name = entry.file_name;
		auto entry_parts = StringUtil::Split(entry_name, '/');

		if (entry_parts.size() < pattern_parts.size()) {
			// This entry is not deep enough to match the pattern
			continue;
		}

		// Check if the pattern matches the entry
		bool match = true;
		for (idx_t i = 0; i < pattern_parts.size(); i++) {
			const auto &pp = pattern_parts[i];
			const auto &ep = entry_parts[i];

			if (pp == "**") {
				// We only allow crawl's to be at the end of the pattern
				if (i != pattern_parts.size() - 1) {
					throw NotImplementedException(
					    "Recursive globs are only supported at the end of tar file path patterns");
				}
				// Otherwise, everything else is a match
				match = true;
				break;
			}

			if (!LikeFun::Glob(ep.c_str(), ep.size(), pp.c_str(), pp.size())) {
				// Not a match
				match = false;
				break;
			}

			if (i == pattern_parts.size() - 1 && entry_parts.size() > pattern_parts.size()) {
				// If the entry is deeper than the pattern (and we havent hit a **), then it is not a match
				match = false;
				break;
			}
		}

		if (match) {
			auto entry_path = "tar://" + tar_path + "/" + entry_name;
			// Cache the offset and size for this file (if it is uncompressed)
			if (cache && is_uncompressed) {
				auto offset = entry.file_offset;
				auto size = entry.header->GetFileSize();
				auto cache_entry = make_shared_ptr<TarArchiveFileMetadataCache>(last_modified, offset, size);
				cache->Put(entry_path, std::move(cache_entry));
			}
			result.push_back(entry_path);
		}
	}

	return result;
}

} // namespace duckdb
