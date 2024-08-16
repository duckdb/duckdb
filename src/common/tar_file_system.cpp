#include "duckdb/common/tar_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Tar Metadata Cache
//------------------------------------------------------------------------------

struct TarArchiveFileMetadataCache final : public ObjectCacheEntry {
public:
	TarArchiveFileMetadataCache() : read_time(0) {
	}
	TarArchiveFileMetadataCache(time_t read_time_p, idx_t byte_offset_p)
	    : read_time(read_time_p), byte_offset(byte_offset_p) {
	}

	//! Read time (of the archive as a whole)
	time_t read_time;

	//! Byte offset to the file in the tar archive
	idx_t byte_offset;

public:
	static string ObjectType() {
		return "tar_archive_metadata";
	}

	string GetObjectType() override {
		return ObjectType();
	}
};

//------------------------------------------------------------------------------
// Tar Scan
//------------------------------------------------------------------------------
struct TarBlockHeader {
	char file_name[100];
	char file_mode[8];
	char owner_id[8];
	char group_id[8];
	char file_size[12];
	char last_modification[12];
	char checksum[8];
	char type[1];
	char linked_file_name[100];
	char ustar[6];
	char ustar_version[2];
	char owner_name[32];
	char group_name[32];
	char device_major[8];
	char device_minor[8];
	char filename_prefix[155];
	char padding[12];

	idx_t GetFileSize() const {
		return strtoul(file_size, nullptr, 8);
	}
};

static_assert(sizeof(TarBlockHeader) == 512, "TarBlockHeader must be 512 bytes");

template <class T>
struct IteratorPair {
	T &begin() {
		return begin_it;
	}
	T &end() {
		return end_it;
	}
	IteratorPair(T begin_it_p, T end_it_p) : begin_it(begin_it_p), end_it(end_it_p) {
	}

private:
	T begin_it;
	T end_it;
};

struct TarBlockIterator {
	TarBlockIterator();
	explicit TarBlockIterator(FileHandle &archive_handle_p)
	    : current_header(make_uniq<TarBlockHeader>()), archive_handle(archive_handle_p) {
		Next();
	}

	TarBlockIterator &operator++() {
		Next();
		return *this;
	}

	const TarBlockHeader &operator*() const {
		return *current_header;
	}

	bool operator==(const TarBlockIterator &other) const {
		return stop == other.stop;
	}

	static IteratorPair<TarBlockIterator> Scan(FileHandle &archive_handle) {
		return {TarBlockIterator(archive_handle), TarBlockIterator()};
	}

private:
	void Next() {
		if (!archive_handle.Read(current_header.get(), sizeof(TarBlockHeader))) {
			stop = true;
			return;
		}
		if (current_header->file_name[0] == 0) {
			stop = true;
			return;
		}
		const auto file_offset = archive_handle.SeekPosition();
		const auto file_size = current_header->GetFileSize();
		const auto file_blocks = (file_size + 511) / 512;
		archive_handle.Seek(file_offset + file_blocks * 512);
	}

private:
	bool stop = false;
	unique_ptr<TarBlockHeader> current_header;
	FileHandle &archive_handle;
};

struct TarEntry {
	string name;
	idx_t byte_offset;

	TarEntry(const string &name, const idx_t byte_offset) : name(name), byte_offset(byte_offset) {
	}
};

static vector<TarEntry> GlobTarFile(FileSystem &fs, string tar_path, optional_ptr<FileOpener> opener) {
	auto file = fs.OpenFile(tar_path, FileFlags::FILE_FLAGS_READ, opener);
	if (!file) {
		throw IOException("Failed to open file: %s", tar_path);
	}

	vector<TarEntry> entries;
	while (true) {
		TarBlockHeader header;
		auto byte_offset = file->SeekPosition();
		if (!file->Read(&header, sizeof(TarBlockHeader))) {
			break;
		}
		if (header.file_name[0] == 0) {
			break;
		}
		entries.emplace_back(header.file_name, byte_offset);
		const auto file_size = header.GetFileSize();
		const auto file_blocks = (file_size + 511) / 512;
		file->Seek(file->SeekPosition() + file_blocks * 512);
	}

	return entries;
}

// Returns the path to the archive file that contains the given file
static pair<string, string> SplitArchivePath(const string &path) {
	constexpr char suffix[] = ".tar";
	auto tar_path = std::find_end(path.begin(), path.end(), suffix, suffix + 4);
	if (tar_path == path.end()) {
		throw IOException("Invalid path: %s", path);
	}

	auto suffix_path = std::next(tar_path, 4);
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
	throw IOException("Invalid path: %s", path);
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
	auto paths = SplitArchivePath(path.substr(6));
	auto &tar_path = paths.first;
	auto &file_path = paths.second;

	// Now we need to find the file within the tar file and return out file handle
	auto handle = parent_file_system.OpenFile(tar_path, flags, opener);

	// TODO: Check the cache to avoid re-reading the tar file

	for (const auto &block : TarBlockIterator::Scan(*handle)) {
		if (block.file_name == file_path) {
			auto start_offset = handle->SeekPosition();
			auto end_offset = start_offset + block.GetFileSize();
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

static shared_ptr<TarArchiveFileMetadataCache> TryGetCachedArchiveMetadata(FileOpener *opener, FileHandle &handle) {
	// Do we have a client context?
	if (!opener) {
		return nullptr;
	}
	auto context = opener->TryGetClientContext();
	if (!context) {
		return nullptr;
	}
	// Is this archive file already in the cache?
	auto &cache = ObjectCache::GetObjectCache(*context);
	auto entry = cache.Get<TarArchiveFileMetadataCache>(handle.path);
	if (!entry) {
		return nullptr;
	}
	// Check if the file has been modified since the last read
	if (handle.file_system.GetLastModifiedTime(handle) >= entry->read_time) {
		return nullptr;
	}
	return entry;
}

vector<string> TarFileSystem::Glob(const string &path, FileOpener *opener) {
	if (!HasGlob(path)) {
		// If there is no glob pattern, just return the path itself
		// TODO: Do a file exists check?
		return {path};
	}

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

	// Given the path to the tar file, open it
	auto entries = GlobTarFile(parent_file_system, tar_path, opener);
	auto archive_handle = parent_file_system.OpenFile(tar_path, FileFlags::FILE_FLAGS_READ, opener);
	if (!archive_handle) {
		throw IOException("Failed to open file: %s", tar_path);
	}

	vector<string> result;
	auto pattern_parts = StringUtil::Split(file_path, '/');

	// TODO: CAche all the entries we pass through

	for (const auto &entry : TarBlockIterator::Scan(*archive_handle)) {
		string entry_name = entry.file_name;

		auto entry_parts = StringUtil::Split(entry_name, '/');

		if (pattern_parts.size() > entry_parts.size()) {
			// This entry is not deep enough to match the pattern
			continue;
		}

		// Check if the pattern matches the entry
		bool match = true;
		for (idx_t i = 0; i < pattern_parts.size(); i++) {
			const auto &pp = pattern_parts[i];
			const auto &ep = entry_parts[i];

			if (IsCrawl(pp)) {
				// Crawl pattern, match everything from here on
				// TODO: Not true.
				break;
			}

			if (!LikeFun::Glob(ep.c_str(), ep.size(), pp.c_str(), pp.size())) {
				// Not a match
				match = false;
				break;
			}
		}
		if (match) {
			result.push_back(JoinPath("tar://" + tar_path, entry_name));
		}
	}

	return result;
}

} // namespace duckdb
