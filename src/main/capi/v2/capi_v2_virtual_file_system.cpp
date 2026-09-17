#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_file_opener.hpp"

#include <unordered_map>

#include <algorithm>

namespace duckdb::capiv2 {

struct CV2VirtualFileSystemCallbacks {
	duckdb_v2_virtual_file_system_can_handle_callback_fn can_handle = nullptr;
	duckdb_v2_virtual_file_system_open_callback_fn open = nullptr;
	duckdb_v2_virtual_file_system_close_callback_fn close = nullptr;
	duckdb_v2_virtual_file_system_abort_callback_fn abort = nullptr;
	duckdb_v2_virtual_file_system_read_at_callback_fn read_at = nullptr;
	duckdb_v2_virtual_file_system_write_at_callback_fn write_at = nullptr;
	duckdb_v2_virtual_file_system_read_callback_fn read = nullptr;
	duckdb_v2_virtual_file_system_write_callback_fn write = nullptr;
	duckdb_v2_virtual_file_system_seek_callback_fn seek = nullptr;
	duckdb_v2_virtual_file_system_tell_callback_fn tell = nullptr;
	duckdb_v2_virtual_file_system_stat_callback_fn stat = nullptr;
	duckdb_v2_virtual_file_system_sync_callback_fn sync = nullptr;
	duckdb_v2_virtual_file_system_truncate_callback_fn truncate = nullptr;
	duckdb_v2_virtual_file_system_stat_path_callback_fn stat_path = nullptr;
	duckdb_v2_virtual_file_system_list_callback_fn list = nullptr;
	duckdb_v2_virtual_file_system_glob_callback_fn glob = nullptr;
	duckdb_v2_virtual_file_system_remove_file_callback_fn remove_file = nullptr;
	duckdb_v2_virtual_file_system_create_directory_callback_fn create_directory = nullptr;
	duckdb_v2_virtual_file_system_remove_directory_callback_fn remove_directory = nullptr;
	duckdb_v2_virtual_file_system_move_callback_fn move = nullptr;
};

// Everything the builder collects. Copied into the engine-side file system at registration, so the builder can be
// changed or destroyed afterwards without affecting what was registered. The user data is shared rather than copied,
// so its destructor runs once both are gone.
struct CV2VirtualFileSystemConfig {
	string name;
	vector<string> prefixes;
	CV2VirtualFileSystemCallbacks callbacks;
	shared_ptr<CV2UserData> user_data;

	bool IsWritable() const {
		return callbacks.write || callbacks.write_at;
	}
	bool CanRead() const {
		return callbacks.read_at != nullptr;
	}
	//! Whether the file system owns the cursor. Otherwise the engine keeps one per file over the offset callbacks.
	bool OwnsCursor() const {
		return callbacks.read || callbacks.write || callbacks.seek || callbacks.tell;
	}
};

class CV2VirtualFileSystem;

// An open file. Doubles as the virtual_file_info handle the per-file callbacks receive. The position is the
// engine's cursor, used only when the file system does not own one.
class CV2VirtualFile final : public FileHandle {
public:
	CV2VirtualFile(CV2VirtualFileSystem &fs, string path, FileOpenFlags flags, CV2UserData data);
	~CV2VirtualFile() override;

	void Close() override;
	bool CanSeek() override {
		return is_seekable;
	}

	void *Data() const {
		return data.GetData();
	}

public:
	CV2UserData data;
	bool closed = false;
	//! The engine's cursor, for a file system that keeps none.
	idx_t position = 0;
	//! Reported by the open callback; see FILE_PROPERTY.
	bool is_seekable = true;
	bool is_on_disk = false;
};

class CV2VirtualFileOpenInfo {
public:
	const OpenFileInfo *file = nullptr;
	FileOpenFlags flags;
	optional_ptr<ClientContext> context;
	optional_ptr<FileOpener> opener;
	optional_ptr<CV2VirtualFileSystem> owner;
	//! The file system this one shadows for the path, built on first request.
	unique_ptr<CV2FileSystem> beneath;
	//! Owned as soon as it is attached, so it is destroyed on every path out of the open.
	CV2UserData data;
	bool is_seekable = true;
	bool is_on_disk = false;
	//! What a listing reported about the file, decoded from the open options on first request.
	unique_ptr<CV2FileStat> listed_stat;
};

class CV2VirtualFilePathInfo {
public:
	const string *path = nullptr;
	//! Only a move has a target.
	const string *target = nullptr;
	optional_ptr<ClientContext> context;
	optional_ptr<FileOpener> opener;
	optional_ptr<CV2VirtualFileSystem> owner;
	//! The file system this one shadows for the path, built on first request.
	unique_ptr<CV2FileSystem> beneath;
};

static auto Convert(duckdb_v2_virtual_file_system_info_handle info) -> CV2VirtualFileSystem * {
	return reinterpret_cast<CV2VirtualFileSystem *>(info);
}
static auto Convert(CV2VirtualFileSystem *fs) -> duckdb_v2_virtual_file_system_info_handle {
	return reinterpret_cast<duckdb_v2_virtual_file_system_info_handle>(fs);
}
static auto Convert(duckdb_v2_virtual_file_open_info_handle info) -> CV2VirtualFileOpenInfo * {
	return reinterpret_cast<CV2VirtualFileOpenInfo *>(info);
}
static auto Convert(CV2VirtualFileOpenInfo *info) -> duckdb_v2_virtual_file_open_info_handle {
	return reinterpret_cast<duckdb_v2_virtual_file_open_info_handle>(info);
}
static auto Convert(duckdb_v2_virtual_file_info_handle info) -> CV2VirtualFile * {
	return reinterpret_cast<CV2VirtualFile *>(info);
}
static auto Convert(CV2VirtualFile *file) -> duckdb_v2_virtual_file_info_handle {
	return reinterpret_cast<duckdb_v2_virtual_file_info_handle>(file);
}
static auto Convert(duckdb_v2_virtual_file_path_info_handle info) -> CV2VirtualFilePathInfo * {
	return reinterpret_cast<CV2VirtualFilePathInfo *>(info);
}
static auto Convert(CV2VirtualFilePathInfo *info) -> duckdb_v2_virtual_file_path_info_handle {
	return reinterpret_cast<duckdb_v2_virtual_file_path_info_handle>(info);
}

// Runs a callback against a fresh error slot, handing back what it reported for the caller to decide on.
template <class FN>
static CV2ErrorInfo TryInvokeCallback(FN &&fn) {
	CV2ErrorInfo err = {};
	auto err_ptr = Convert(&err);
	fn(err_ptr);
	return err;
}

// Runs a callback against a fresh error slot and rethrows whatever it reported.
template <class FN>
static void InvokeCallback(FN &&fn) {
	auto err = TryInvokeCallback(std::forward<FN>(fn));
	if (err.HasError()) {
		err.ThrowAsException();
	}
}

// The live bridge file systems, so a consumer can tell one apart from the engine's without RTTI.
static mutex &ProviderRegistryLock() {
	static mutex lock;
	return lock;
}
static std::unordered_map<const FileSystem *, CV2PathStatProvider *> &ProviderRegistry() {
	static std::unordered_map<const FileSystem *, CV2PathStatProvider *> registry;
	return registry;
}

auto FindPathStatProvider(const FileSystem &fs) -> optional_ptr<CV2PathStatProvider> {
	lock_guard<mutex> guard(ProviderRegistryLock());
	auto &registry = ProviderRegistry();
	auto entry = registry.find(&fs);
	return entry == registry.end() ? nullptr : entry->second;
}

// The engine-side file system: routes every engine call to the matching callback, and fills in what the callbacks
// leave out (the engine's cursor, plain-path globs, directory globs over listings, try-removal).
class CV2VirtualFileSystem final : public FileSystem, public CV2PathStatProvider {
public:
	CV2VirtualFileSystem(CV2VirtualFileSystemConfig config_p, DatabaseInstance &db)
	    : config(std::move(config_p)), db(db) {
		lock_guard<mutex> guard(ProviderRegistryLock());
		ProviderRegistry()[this] = this;
	}

	~CV2VirtualFileSystem() override {
		lock_guard<mutex> guard(ProviderRegistryLock());
		ProviderRegistry().erase(this);
	}

	string GetName() const override {
		return config.name;
	}

	bool CanHandleFile(const string &path) override {
		for (auto &prefix : config.prefixes) {
			if (StringUtil::StartsWith(path, prefix)) {
				return true;
			}
		}
		if (!config.callbacks.can_handle) {
			return false;
		}
		bool result = false;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			config.callbacks.can_handle(Convert(this), Convert(path), &result, &err);
		});
		return result;
	}

	//! The database's file system as a borrowed consumer handle, for callbacks that delegate without a context. The
	//! engine's wrapper pushes the database's own opener, so requests through it see database-level settings and
	//! secrets.
	CV2FileSystem &DelegateSlot() {
		lock_guard<mutex> guard(delegate_lock);
		if (!delegate) {
			delegate = make_shared_ptr<CV2FileSystem>();
			delegate->fs = &db.GetFileSystem();
			delegate->router = &GetRouter(db);
		}
		return *delegate;
	}

	//! A consumer handle on the file system this one shadows for a path, carrying the request's opener, or the
	//! database's when the request has none.
	unique_ptr<CV2FileSystem> BeneathSlot(const string &path, optional_ptr<ClientContext> context,
	                                      optional_ptr<FileOpener> opener) {
		auto slot = make_uniq<CV2FileSystem>();
		slot->fs = &GetRouter(db).GetFileSystemBeneath(path, *this);
		slot->opener = opener;
		if (!slot->opener && context) {
			slot->opener = ClientData::Get(*context).file_opener.get();
		}
		if (!slot->opener) {
			slot->opener = static_cast<DatabaseFileSystem &>(db.GetFileSystem()).GetOpener();
		}
		if (context) {
			slot->query = QueryContext(*context);
		}
		return slot;
	}

	bool TryStatPath(const string &path, optional_ptr<FileOpener> opener, CV2FileStat &result) override {
		if (!config.callbacks.stat_path) {
			return false;
		}
		result = StatPath(path, opener);
		return true;
	}

	//===--------------------------------------------------------------------===//
	// Open / close
	//===--------------------------------------------------------------------===//
	bool SupportsOpenFileExtended() const override {
		return true;
	}

	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		if (flags.OpenForWriting() || flags.OpenForAppending()) {
			if (!config.IsWritable()) {
				throw PermissionException("File system \"%s\" is read-only: it has no write callback, so \"%s\" cannot "
				                          "be opened for writing",
				                          config.name, file.path);
			}
			if (flags.RequireParallelAccess() && !cb.write_at) {
				throw NotImplementedException("File system \"%s\" has no \"write at\" callback, which \"%s\" needs "
				                              "since it is opened for parallel access",
				                              config.name, file.path);
			}
		}
		if (flags.OpenForReading() && !config.CanRead()) {
			throw PermissionException("File system \"%s\" is write-only: it has no \"read at\" callback, so \"%s\" "
			                          "cannot be opened for reading",
			                          config.name, file.path);
		}

		CV2VirtualFileOpenInfo info;
		info.file = &file;
		info.flags = flags;
		info.context = FileOpener::TryGetClientContext(opener);
		info.opener = opener;
		info.owner = this;

		auto err =
		    TryInvokeCallback([&](duckdb_v2_error_info_handle err) { cb.open(Convert(this), Convert(&info), &err); });
		if (err.HasError()) {
			// Whatever the callback attached is destroyed with `info`.
			if (err.code == DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND && flags.ReturnNullIfNotExists()) {
				return nullptr;
			}
			err.ThrowAsException();
		}

		if (!info.is_seekable && !config.OwnsCursor()) {
			// The engine's cursor is a position in the file, which a stream does not have. `info` releases the state.
			throw NotImplementedException("File system \"%s\" reports \"%s\" as not seekable, which only a file "
			                              "system that owns the cursor can serve",
			                              config.name, file.path);
		}
		auto handle = make_uniq<CV2VirtualFile>(*this, file.path, flags, std::move(info.data));
		handle->is_seekable = info.is_seekable;
		handle->is_on_disk = info.is_on_disk;
		if (flags.OpenForAppending() && !config.OwnsCursor()) {
			// The engine's cursor starts at the end; the file system's own cursor is its business.
			handle->position = NumericCast<idx_t>(GetFileSize(*handle));
		}
		return std::move(handle);
	}

	static bool OpenedForWriting(const CV2VirtualFile &handle) {
		return handle.flags.OpenForWriting() || handle.flags.OpenForAppending();
	}

	void CloseFile(CV2VirtualFile &handle) {
		// A written file is always synced before it is closed; the engine's writers do not promise to.
		if (OpenedForWriting(handle)) {
			FileSync(handle);
		}
		if (!config.callbacks.close) {
			return;
		}
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { config.callbacks.close(Convert(this), Convert(&handle), &err); });
	}

	bool CanAbort() const {
		return config.callbacks.abort != nullptr;
	}

	void AbortFileWrite(FileHandle &handle_p) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		if (!CanAbort() || handle.closed) {
			return;
		}
		// Abort takes the place of close, so the close callback never sees an aborted file.
		handle.closed = true;
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { config.callbacks.abort(Convert(this), Convert(&handle), &err); });
	}

	//===--------------------------------------------------------------------===//
	// I/O
	//===--------------------------------------------------------------------===//
	void Read(FileHandle &handle_p, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		auto total = NumericCast<idx_t>(nr_bytes);
		auto *out = static_cast<data_ptr_t>(buffer);
		// The callback may come up short; the engine's contract here is all or nothing.
		idx_t done = 0;
		while (done < total) {
			auto read = ReadAt(handle, out + done, total - done, location + done);
			if (read == 0) {
				throw IOException("Could not read all bytes from file \"%s\": wanted %llu bytes at offset %llu, but "
				                  "the file ended after %llu",
				                  handle.path, total, location, done);
			}
			done += read;
		}
	}

	void Write(FileHandle &handle_p, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		auto &cb = config.callbacks;
		RequireCallback(cb.write_at, "write at");
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.write_at(Convert(this), Convert(&handle), buffer, NumericCast<idx_t>(nr_bytes), location, &err);
		});
	}

	int64_t Read(FileHandle &handle_p, void *buffer, int64_t nr_bytes) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		auto &cb = config.callbacks;
		auto count = NumericCast<idx_t>(nr_bytes);
		if (!config.OwnsCursor()) {
			// One offset read at the engine's cursor: short reads and a zero at the end are what it wants.
			auto bytes_read = ReadAt(handle, buffer, count, handle.position);
			handle.position += bytes_read;
			return NumericCast<int64_t>(bytes_read);
		}
		RequireCallback(cb.read, "read");
		idx_t bytes_read = 0;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.read(Convert(this), Convert(&handle), buffer, count, &bytes_read, &err);
		});
		return NumericCast<int64_t>(bytes_read);
	}

	int64_t Write(FileHandle &handle_p, void *buffer, int64_t nr_bytes) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		auto &cb = config.callbacks;
		if (!config.OwnsCursor()) {
			Write(handle, buffer, nr_bytes, handle.position);
			handle.position += NumericCast<idx_t>(nr_bytes);
			return nr_bytes;
		}
		RequireCallback(cb.write, "write");
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.write(Convert(this), Convert(&handle), buffer, NumericCast<idx_t>(nr_bytes), &err);
		});
		return nr_bytes;
	}

	void Seek(FileHandle &handle_p, idx_t location) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		auto &cb = config.callbacks;
		if (!config.OwnsCursor()) {
			handle.position = location;
			return;
		}
		RequireCallback(cb.seek, "seek");
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { cb.seek(Convert(this), Convert(&handle), location, &err); });
	}

	idx_t SeekPosition(FileHandle &handle_p) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		auto &cb = config.callbacks;
		if (!config.OwnsCursor()) {
			return handle.position;
		}
		RequireCallback(cb.tell, "tell");
		idx_t position = 0;
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { cb.tell(Convert(this), Convert(&handle), &position, &err); });
		return position;
	}

	int64_t GetFileSize(FileHandle &handle) override {
		auto stat = Stat(handle.Cast<CV2VirtualFile>());
		if (!stat.size) {
			throw IOException("The size of \"%s\" is not known to file system \"%s\", and this operation needs it",
			                  handle.path, config.name);
		}
		return NumericCast<int64_t>(*stat.size);
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return Stat(handle.Cast<CV2VirtualFile>()).LastModified();
	}

	string GetVersionTag(FileHandle &handle) override {
		auto stat = Stat(handle.Cast<CV2VirtualFile>());
		return stat.version_tag ? *stat.version_tag : string();
	}

	static FileType ToFileType(DUCKDB_V2_FILE_TYPE type) {
		// An open file is a regular file unless the stat callback says it is a pipe.
		return type == DUCKDB_V2_FILE_TYPE_PIPE ? FileType::FILE_TYPE_FIFO : FileType::FILE_TYPE_REGULAR;
	}

	FileType GetFileType(FileHandle &handle) override {
		return ToFileType(Stat(handle.Cast<CV2VirtualFile>()).type);
	}

	FileMetadata Stats(FileHandle &handle) override {
		auto stat = Stat(handle.Cast<CV2VirtualFile>());
		FileMetadata metadata;
		metadata.file_size = stat.size ? NumericCast<int64_t>(*stat.size) : -1;
		metadata.last_modification_time = stat.LastModified();
		metadata.file_type = ToFileType(stat.type);
		if (stat.version_tag) {
			metadata.version_tag = *stat.version_tag;
		}
		return metadata;
	}

	void FileSync(FileHandle &handle_p) override {
		if (!config.callbacks.sync) {
			return;
		}
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { config.callbacks.sync(Convert(this), Convert(&handle), &err); });
	}

	void Truncate(FileHandle &handle_p, int64_t new_size) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		auto &cb = config.callbacks;
		RequireCallback(cb.truncate, "truncate");
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.truncate(Convert(this), Convert(&handle), NumericCast<idx_t>(new_size), &err);
		});
	}

	bool OnDiskFile(FileHandle &handle) override {
		return handle.Cast<CV2VirtualFile>().is_on_disk;
	}

	bool CanSeek() override {
		// Asked without a file in hand; the callers that matter ask the handle, which knows.
		return true;
	}

	//===--------------------------------------------------------------------===//
	// Paths
	//===--------------------------------------------------------------------===//
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override {
		return StatPath(filename, opener).type == DUCKDB_V2_FILE_TYPE_REGULAR;
	}

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override {
		return StatPath(directory, opener).type == DUCKDB_V2_FILE_TYPE_DIRECTORY;
	}

	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override {
		// Asked on every open; a file system that cannot stat paths has no pipes to report.
		if (!config.callbacks.stat_path) {
			return false;
		}
		return StatPath(filename, opener).type == DUCKDB_V2_FILE_TYPE_PIPE;
	}

	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		RequireCallback(cb.remove_file, "remove file");
		auto path = PathInfo(filename, opener);
		InvokeCallback([&](duckdb_v2_error_info_handle err) { cb.remove_file(Convert(this), Convert(&path), &err); });
	}

	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		RequireCallback(cb.remove_file, "remove file");
		auto path = PathInfo(filename, opener);
		auto err = TryInvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { cb.remove_file(Convert(this), Convert(&path), &err); });
		if (!err.HasError()) {
			return true;
		}
		if (err.code == DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND) {
			return false;
		}
		err.ThrowAsException();
	}

	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) override {
		CreateDirectoryExtended(directory, {CreateDirectoryMode::SINGLE}, opener);
	}

	bool CreateDirectoryExtended(const string &directory, const CreateDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		RequireCallback(cb.create_directory, "create directory");
		auto path = PathInfo(directory, opener);
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { cb.create_directory(Convert(this), Convert(&path), &err); });
		return true;
	}

	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) override {
		RemoveDirectoryExtended(directory, {RemoveDirectoryMode::RECURSIVE}, opener);
	}

	bool RemoveDirectoryExtended(const string &directory, const RemoveDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		RequireCallback(cb.remove_directory, "remove directory");
		auto path = PathInfo(directory, opener);
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { cb.remove_directory(Convert(this), Convert(&path), &err); });
		return true;
	}

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		RequireCallback(cb.move, "move");
		// The engine routes a move by its source alone.
		if (!CanHandleFile(target)) {
			throw NotImplementedException("Cannot move \"%s\" to \"%s\": the target is not on file system \"%s\"",
			                              source, target, config.name);
		}
		auto path = PathInfo(source, opener);
		path.target = &target;
		InvokeCallback([&](duckdb_v2_error_info_handle err) { cb.move(Convert(this), Convert(&path), &err); });
	}

	bool SupportsListFilesExtended() const override {
		return true;
	}

	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override {
		auto listing = List(directory, opener);
		for (auto &entry : listing.entries) {
			auto info = ToOpenFileInfo(entry);
			callback(info);
		}
		return true;
	}

	bool SupportsGlobExtended() const override {
		return true;
	}

	unique_ptr<MultiFileList> GlobFilesExtended(const string &path, const FileGlobInput &input,
	                                            optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		vector<OpenFileInfo> result;
		if (cb.glob) {
			CV2FileListing listing;
			auto pattern = PathInfo(path, opener);
			InvokeCallback([&](duckdb_v2_error_info_handle err) {
				cb.glob(Convert(this), Convert(&pattern), Convert(&listing), &err);
			});
			for (auto &entry : listing.entries) {
				result.push_back(ToOpenFileInfo(entry));
			}
		} else if (!HasGlob(path)) {
			// A plain path names one file. Without a stat callback there is no way to check, so let the open fail.
			if (!cb.stat_path || FileExists(path, opener)) {
				result.emplace_back(path);
			}
		} else if (cb.list) {
			ExpandGlob(path, opener, result);
		} else {
			throw NotImplementedException("File system \"%s\" has no list or glob callback, so the pattern \"%s\" "
			                              "cannot be expanded",
			                              config.name, path);
		}
		return make_uniq<SimpleMultiFileList>(std::move(result));
	}

public:
	CV2VirtualFileSystemConfig config;

private:
	DatabaseInstance &db;
	mutex delegate_lock;
	shared_ptr<CV2FileSystem> delegate;

	template <class FN>
	void RequireCallback(FN callback, const char *what) const {
		if (!callback) {
			throw NotImplementedException("File system \"%s\" has no \"%s\" callback", config.name, what);
		}
	}

	CV2VirtualFilePathInfo PathInfo(const string &path, optional_ptr<FileOpener> opener) {
		CV2VirtualFilePathInfo info;
		info.path = &path;
		info.context = FileOpener::TryGetClientContext(opener);
		info.opener = opener;
		info.owner = this;
		return info;
	}

	static OpenFileInfo ToOpenFileInfo(const CV2FileListing::Entry &entry) {
		OpenFileInfo info(entry.path);
		info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		auto &options = info.extended_info->options;
		options.emplace("type", Value(entry.type == DUCKDB_V2_FILE_TYPE_DIRECTORY ? "directory" : "file"));
		entry.stat.FillOptions(options);
		return info;
	}

	idx_t ReadAt(CV2VirtualFile &handle, void *buffer, idx_t count, idx_t location) {
		auto &cb = config.callbacks;
		RequireCallback(cb.read_at, "read at");
		idx_t bytes_read = 0;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.read_at(Convert(this), Convert(&handle), buffer, count, location, &bytes_read, &err);
		});
		if (bytes_read > count) {
			throw IOException("The read at callback of file system \"%s\" reported %llu bytes read into a buffer "
			                  "of %llu",
			                  config.name, bytes_read, count);
		}
		return bytes_read;
	}

	CV2FileStat Stat(CV2VirtualFile &handle) {
		CV2FileStat info;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			config.callbacks.stat(Convert(this), Convert(&handle), Convert(&info), &err);
		});
		return info;
	}

	CV2FileStat StatPath(const string &path_p, optional_ptr<FileOpener> opener) {
		auto &cb = config.callbacks;
		RequireCallback(cb.stat_path, "stat path");
		auto path = PathInfo(path_p, opener);
		CV2FileStat info;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.stat_path(Convert(this), Convert(&path), Convert(&info), &err);
		});
		return info;
	}

	CV2FileListing List(const string &directory, optional_ptr<FileOpener> opener) {
		auto &cb = config.callbacks;
		RequireCallback(cb.list, "list");
		auto path = PathInfo(directory, opener);
		CV2FileListing listing;
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { cb.list(Convert(this), Convert(&path), Convert(&listing), &err); });
		return listing;
	}

	//===--------------------------------------------------------------------===//
	// Generic glob over the list callback
	//===--------------------------------------------------------------------===//
	static string JoinSegment(const string &base, const string &segment) {
		if (base.empty() || StringUtil::EndsWith(base, "/")) {
			return base + segment;
		}
		return base + "/" + segment;
	}

	static bool MatchesSegment(const string &name, const string &pattern) {
		return duckdb::Glob(name.c_str(), name.size(), pattern.c_str(), pattern.size());
	}

	// Splits a pattern into a root that is never matched against (a scheme, or a leading separator) and the
	// components after it.
	static void SplitPattern(const string &pattern, string &root, vector<string> &segments) {
		auto scheme_end = pattern.find("://");
		string rest;
		if (scheme_end != string::npos) {
			root = pattern.substr(0, scheme_end + 3);
			rest = pattern.substr(scheme_end + 3);
		} else if (StringUtil::StartsWith(pattern, "/")) {
			root = "/";
			rest = pattern.substr(1);
		} else {
			rest = pattern;
		}
		for (auto &segment : StringUtil::Split(rest, '/')) {
			if (!segment.empty()) {
				segments.push_back(std::move(segment));
			}
		}
	}

	void ExpandGlob(const string &pattern, optional_ptr<FileOpener> opener, vector<OpenFileInfo> &result) {
		string root;
		vector<string> segments;
		SplitPattern(pattern, root, segments);
		if (segments.empty()) {
			return;
		}
		ExpandSegments(root, segments, 0, opener, result);
		std::sort(result.begin(), result.end());
	}

	// Expands segments[index..] inside the directory `current`, appending every matching file to `result`.
	void ExpandSegments(const string &current, const vector<string> &segments, idx_t index,
	                    optional_ptr<FileOpener> opener, vector<OpenFileInfo> &result) {
		auto &segment = segments[index];
		const bool is_last = index + 1 == segments.size();

		if (segment == "**") {
			// Matches any number of directories, including none.
			if (!is_last) {
				ExpandSegments(current, segments, index + 1, opener, result);
			}
			for (auto &entry : List(current, opener).entries) {
				auto child = JoinSegment(current, entry.path);
				if (entry.type == DUCKDB_V2_FILE_TYPE_DIRECTORY) {
					ExpandSegments(child, segments, index, opener, result);
				} else if (is_last) {
					entry.path = std::move(child);
					result.push_back(ToOpenFileInfo(entry));
				}
			}
			return;
		}

		if (!HasGlob(segment)) {
			auto child = JoinSegment(current, segment);
			if (!is_last) {
				ExpandSegments(child, segments, index + 1, opener, result);
			} else if (!config.callbacks.stat_path || FileExists(child, opener)) {
				result.emplace_back(std::move(child));
			}
			return;
		}

		for (auto &entry : List(current, opener).entries) {
			if (!MatchesSegment(entry.path, segment)) {
				continue;
			}
			auto child = JoinSegment(current, entry.path);
			if (is_last) {
				if (entry.type == DUCKDB_V2_FILE_TYPE_REGULAR) {
					entry.path = std::move(child);
					result.push_back(ToOpenFileInfo(entry));
				}
			} else if (entry.type == DUCKDB_V2_FILE_TYPE_DIRECTORY) {
				ExpandSegments(child, segments, index + 1, opener, result);
			}
		}
	}
};

CV2VirtualFile::CV2VirtualFile(CV2VirtualFileSystem &fs, string path_p, FileOpenFlags flags_p, CV2UserData data_p)
    : FileHandle(fs, std::move(path_p), flags_p), data(std::move(data_p)) {
}

CV2VirtualFile::~CV2VirtualFile() {
	// Reached unclosed only when an error unwound past whoever was writing: abandon the write if we can.
	try {
		auto &fs = file_system.Cast<CV2VirtualFileSystem>();
		if (!closed && CV2VirtualFileSystem::OpenedForWriting(*this) && fs.CanAbort()) {
			fs.AbortFileWrite(*this);
		} else {
			Close();
		}
	} catch (...) { // NOLINT: a destructor must not throw
	}
}

void CV2VirtualFile::Close() {
	if (closed) {
		return;
	}
	closed = true;
	file_system.Cast<CV2VirtualFileSystem>().CloseFile(*this);
}

// The builder. Holds the configuration until registration copies it into a fresh engine-side file system.
class CV2VirtualFileSystemBuilder {
public:
	explicit CV2VirtualFileSystemBuilder(DatabaseInstance &db) : db(db) {
	}

	void Register() {
		auto &cb = config.callbacks;
		if (config.name.empty()) {
			throw InvalidInputException("File system name cannot be empty.");
		}
		if (config.prefixes.empty() && !cb.can_handle) {
			throw InvalidInputException("A prefix or a can handle callback must be set for the file system.");
		}
		if (!cb.open) {
			throw InvalidInputException("Open callback must be set for the file system.");
		}
		if (!config.CanRead() && !config.IsWritable()) {
			throw InvalidInputException(
			    "Read at callback must be set for the file system, unless it is a write-only sink with a write "
			    "or write at callback.");
		}
		if (!cb.stat) {
			throw InvalidInputException("Stat callback must be set for the file system.");
		}
		// Owning the cursor means saying where it is, and reading or writing it as far as the file system does
		// either at all; seek is per-file business.
		if (config.OwnsCursor()) {
			if (!cb.tell) {
				throw InvalidInputException("Tell callback must be set for a file system that owns the cursor by "
				                            "setting any of the read, write, seek or tell callbacks.");
			}
			if (config.CanRead() && !cb.read) {
				throw InvalidInputException(
				    "Read callback must be set for a file system that owns the cursor and has a read at callback.");
			}
			if (cb.write_at && !cb.write) {
				throw InvalidInputException(
				    "Write callback must be set for a file system that owns the cursor and has a write at callback.");
			}
		}
		auto fs = make_uniq<CV2VirtualFileSystem>(config, db);
		FileSystem::GetFileSystem(db).RegisterSubSystem(std::move(fs));
	}

public:
	CV2VirtualFileSystemConfig config;

private:
	DatabaseInstance &db;
};

static auto Convert(duckdb_v2_virtual_file_system_handle fs) -> CV2VirtualFileSystemBuilder * {
	return reinterpret_cast<CV2VirtualFileSystemBuilder *>(fs);
}
static auto Convert(CV2VirtualFileSystemBuilder *fs) -> duckdb_v2_virtual_file_system_handle {
	return reinterpret_cast<duckdb_v2_virtual_file_system_handle>(fs);
}

// The engine bit behind one C flag. The C enum is a list of names rather than a bitmask, so each value maps to
// exactly one engine flag and anything else is a caller error.
static idx_t FileFlagBit(DUCKDB_V2_FILE_FLAG flag) {
	switch (flag) {
	case DUCKDB_V2_FILE_FLAG_READ:
		return FileOpenFlags::FILE_FLAGS_READ;
	case DUCKDB_V2_FILE_FLAG_WRITE:
		return FileOpenFlags::FILE_FLAGS_WRITE;
	case DUCKDB_V2_FILE_FLAG_CREATE:
		return FileOpenFlags::FILE_FLAGS_FILE_CREATE;
	case DUCKDB_V2_FILE_FLAG_CREATE_NEW:
		return FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW;
	case DUCKDB_V2_FILE_FLAG_APPEND:
		return FileOpenFlags::FILE_FLAGS_APPEND;
	case DUCKDB_V2_FILE_FLAG_EXCLUSIVE_CREATE:
		return FileOpenFlags::FILE_FLAGS_EXCLUSIVE_CREATE;
	case DUCKDB_V2_FILE_FLAG_PARALLEL_ACCESS:
		return FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS;
	default:
		// Includes FILE_FLAG_INVALID, which names no behaviour.
		throw InvalidInputException("'%d' is not a file flag.", static_cast<int>(flag));
	}
}

static bool HasFlag(const FileOpenFlags &flags, DUCKDB_V2_FILE_FLAG flag) {
	return (flags.GetFlagsInternal() & FileFlagBit(flag)) != 0;
}

static auto ContextHandle(optional_ptr<ClientContext> context) -> duckdb_v2_context_handle {
	return context ? Convert(context.get()) : nullptr;
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_create_with_connection(duckdb_v2_connection_handle connection,
                                                                     duckdb_v2_virtual_file_system_handle *file_system,
                                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(connection);
	DUCKDB_CHECK_ARG(file_system);
	*file_system = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &db = *Convert(connection)->context->db;
		auto builder = duckdb::make_uniq<CV2VirtualFileSystemBuilder>(db);
		*file_system = Convert(builder.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_create_with_extension(duckdb_v2_extension_handle extension,
                                                                    duckdb_v2_virtual_file_system_handle *file_system,
                                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(file_system);
	*file_system = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &db = GetExtensionLoader(extension).GetDatabaseInstance();
		auto builder = duckdb::make_uniq<CV2VirtualFileSystemBuilder>(db);
		*file_system = Convert(builder.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_name(duckdb_v2_virtual_file_system_handle file_system,
                                                       duckdb_v2_str name, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(name);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.name = duckdb::string(Convert(name)); });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_add_prefix(duckdb_v2_virtual_file_system_handle file_system,
                                                         duckdb_v2_str prefix, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(prefix);
	return WithErrorHandler(err, [&]() {
		auto value = duckdb::string(Convert(prefix));
		if (value.empty()) {
			throw duckdb::InvalidInputException("A file system prefix cannot be empty.");
		}
		Convert(file_system)->config.prefixes.push_back(std::move(value));
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_user_data(duckdb_v2_virtual_file_system_handle file_system,
                                                            duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(file_system)->config.user_data =
		    duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR
duckdb_v2_virtual_file_system_set_can_handle_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                      duckdb_v2_virtual_file_system_can_handle_callback_fn callback,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.can_handle = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_open_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                                duckdb_v2_virtual_file_system_open_callback_fn callback,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.open = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_virtual_file_system_set_close_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                 duckdb_v2_virtual_file_system_close_callback_fn callback,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.close = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_virtual_file_system_set_abort_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                 duckdb_v2_virtual_file_system_abort_callback_fn callback,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.abort = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_virtual_file_system_set_read_at_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                   duckdb_v2_virtual_file_system_read_at_callback_fn callback,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.read_at = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_virtual_file_system_set_write_at_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                    duckdb_v2_virtual_file_system_write_at_callback_fn callback,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.write_at = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_read_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                                duckdb_v2_virtual_file_system_read_callback_fn callback,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.read = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_virtual_file_system_set_write_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                 duckdb_v2_virtual_file_system_write_callback_fn callback,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.write = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_seek_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                                duckdb_v2_virtual_file_system_seek_callback_fn callback,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.seek = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_tell_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                                duckdb_v2_virtual_file_system_tell_callback_fn callback,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.tell = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_stat_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                                duckdb_v2_virtual_file_system_stat_callback_fn callback,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.stat = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_sync_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                                duckdb_v2_virtual_file_system_sync_callback_fn callback,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.sync = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_virtual_file_system_set_truncate_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                    duckdb_v2_virtual_file_system_truncate_callback_fn callback,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.truncate = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_virtual_file_system_set_stat_path_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                     duckdb_v2_virtual_file_system_stat_path_callback_fn callback,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.stat_path = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_list_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                                duckdb_v2_virtual_file_system_list_callback_fn callback,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.list = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_glob_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                                duckdb_v2_virtual_file_system_glob_callback_fn callback,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.glob = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_virtual_file_system_set_remove_file_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                       duckdb_v2_virtual_file_system_remove_file_callback_fn callback,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.remove_file = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_create_directory_callback(
    duckdb_v2_virtual_file_system_handle file_system,
    duckdb_v2_virtual_file_system_create_directory_callback_fn callback, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.create_directory = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_remove_directory_callback(
    duckdb_v2_virtual_file_system_handle file_system,
    duckdb_v2_virtual_file_system_remove_directory_callback_fn callback, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.remove_directory = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_set_move_callback(duckdb_v2_virtual_file_system_handle file_system,
                                                                duckdb_v2_virtual_file_system_move_callback_fn callback,
                                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.move = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_info_get_user_data(duckdb_v2_virtual_file_system_info_handle info,
                                                                 void **data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		auto &user_data = Convert(info)->config.user_data;
		*data = user_data ? user_data->GetData() : nullptr;
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_info_get_file_system(duckdb_v2_virtual_file_system_info_handle info,
                                                                   duckdb_v2_file_system_handle *file_system,
                                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(file_system);
	*file_system = nullptr;
	return WithErrorHandler(err, [&]() { *file_system = Convert(&Convert(info)->DelegateSlot()); });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_open_info_get_path(duckdb_v2_virtual_file_open_info_handle info,
                                                          duckdb_v2_str *path, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(path);
	return WithErrorHandler(err, [&]() { *path = Convert(Convert(info)->file->path); });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_open_info_has_flag(duckdb_v2_virtual_file_open_info_handle info,
                                                          DUCKDB_V2_FILE_FLAG flag, bool *has_flag,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(has_flag);
	*has_flag = false;
	return WithErrorHandler(err, [&]() { *has_flag = HasFlag(Convert(info)->flags, flag); });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_open_info_get_value(duckdb_v2_virtual_file_open_info_handle info,
                                                           duckdb_v2_str name, duckdb_v2_value_handle *value,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(value);
	*value = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &extended_info = Convert(info)->file->extended_info;
		if (!extended_info) {
			return;
		}
		auto entry = extended_info->options.find(duckdb::string(Convert(name)));
		if (entry == extended_info->options.end()) {
			return;
		}
		*value = Convert(new duckdb::Value(entry->second));
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_open_info_get_context(duckdb_v2_virtual_file_open_info_handle info,
                                                             duckdb_v2_context_handle *context,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(context);
	*context = nullptr;
	return WithErrorHandler(err, [&]() { *context = ContextHandle(Convert(info)->context); });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_open_info_get_stat(duckdb_v2_virtual_file_open_info_handle info,
                                                          duckdb_v2_file_stat_handle *stat_info,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(stat_info);
	*stat_info = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &open_info = *Convert(info);
		if (!open_info.listed_stat) {
			open_info.listed_stat = duckdb::make_uniq<CV2FileStat>(CV2FileStat::FromOptions(*open_info.file));
		}
		*stat_info = Convert(open_info.listed_stat.get());
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_open_info_get_options(duckdb_v2_virtual_file_open_info_handle info,
                                                             duckdb_v2_file_open_options_handle *options,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(options);
	*options = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &open_info = *Convert(info);
		auto copy = duckdb::make_uniq<CV2FileOpenOptions>();
		copy->flags = open_info.flags;
		copy->has_flags = true;
		if (open_info.file->extended_info) {
			// A copy of the values, so adjusting them afterwards does not touch the request.
			copy->extended_info = duckdb::make_shared_ptr<duckdb::ExtendedOpenFileInfo>(*open_info.file->extended_info);
		}
		*options = Convert(copy.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_open_info_get_file_system_beneath(duckdb_v2_virtual_file_open_info_handle info,
                                                                         duckdb_v2_file_system_handle *file_system,
                                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(file_system);
	*file_system = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &open_info = *Convert(info);
		if (!open_info.beneath) {
			open_info.beneath = open_info.owner->BeneathSlot(open_info.file->path, open_info.context, open_info.opener);
		}
		*file_system = Convert(open_info.beneath.get());
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_open_info_set_file_data(duckdb_v2_virtual_file_open_info_handle info,
                                                               duckdb_v2_opaque *data,
                                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		// Assigning destroys whatever was attached before.
		Convert(info)->data = CV2UserData(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_open_info_set_property(duckdb_v2_virtual_file_open_info_handle info,
                                                              DUCKDB_V2_FILE_PROPERTY property, bool value,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		switch (property) {
		case DUCKDB_V2_FILE_PROPERTY_IS_SEEKABLE:
			Convert(info)->is_seekable = value;
			break;
		case DUCKDB_V2_FILE_PROPERTY_IS_ON_DISK:
			Convert(info)->is_on_disk = value;
			break;
		default:
			throw duckdb::InvalidInputException("'%d' is not a file property.", static_cast<int>(property));
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_info_get_file_data(duckdb_v2_virtual_file_info_handle info, void **data,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->Data(); });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_info_get_path(duckdb_v2_virtual_file_info_handle info, duckdb_v2_str *path,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(path);
	return WithErrorHandler(err, [&]() { *path = Convert(Convert(info)->path); });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_info_has_flag(duckdb_v2_virtual_file_info_handle info, DUCKDB_V2_FILE_FLAG flag,
                                                     bool *has_flag, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(has_flag);
	*has_flag = false;
	return WithErrorHandler(err, [&]() { *has_flag = HasFlag(Convert(info)->flags, flag); });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_path_info_get_path(duckdb_v2_virtual_file_path_info_handle info,
                                                          duckdb_v2_str *path, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(path);
	return WithErrorHandler(err, [&]() { *path = Convert(*Convert(info)->path); });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_path_info_get_target_path(duckdb_v2_virtual_file_path_info_handle info,
                                                                 duckdb_v2_str *path,
                                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(path);
	return WithErrorHandler(err, [&]() {
		auto target = Convert(info)->target;
		if (!target) {
			throw duckdb::InvalidInputException("Only a move has a target path.");
		}
		*path = Convert(*target);
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_path_info_get_context(duckdb_v2_virtual_file_path_info_handle info,
                                                             duckdb_v2_context_handle *context,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(context);
	*context = nullptr;
	return WithErrorHandler(err, [&]() { *context = ContextHandle(Convert(info)->context); });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_path_info_get_file_system_beneath(duckdb_v2_virtual_file_path_info_handle info,
                                                                         duckdb_v2_file_system_handle *file_system,
                                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(file_system);
	*file_system = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &path_info = *Convert(info);
		if (!path_info.beneath) {
			path_info.beneath = path_info.owner->BeneathSlot(*path_info.path, path_info.context, path_info.opener);
		}
		*file_system = Convert(path_info.beneath.get());
	});
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_register(duckdb_v2_virtual_file_system_handle file_system,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->Register(); });
}

DUCKDB_V2_ERROR duckdb_v2_virtual_file_system_destroy(duckdb_v2_virtual_file_system_handle *file_system) {
	return WithErrorHandler(nullptr, [&]() {
		if (!file_system) {
			return;
		}
		if (*file_system) {
			delete Convert(*file_system);
			*file_system = nullptr;
		}
	});
}
