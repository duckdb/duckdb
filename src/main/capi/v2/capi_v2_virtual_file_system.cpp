#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb::capiv2 {

struct CV2VirtualFileSystemCallbacks {
	duckdb_v2_vfs_claim_callback_fn claim = nullptr;
	duckdb_v2_vfs_file_open_callback_fn open = nullptr;
	duckdb_v2_vfs_file_close_callback_fn close = nullptr;
	duckdb_v2_vfs_file_abort_callback_fn abort = nullptr;
	duckdb_v2_vfs_file_read_at_callback_fn read_at = nullptr;
	duckdb_v2_vfs_file_write_at_callback_fn write_at = nullptr;
	duckdb_v2_vfs_file_read_callback_fn read = nullptr;
	duckdb_v2_vfs_file_write_callback_fn write = nullptr;
	duckdb_v2_vfs_file_seek_callback_fn seek = nullptr;
	duckdb_v2_vfs_file_tell_callback_fn tell = nullptr;
	duckdb_v2_vfs_file_stat_callback_fn stat = nullptr;
	duckdb_v2_vfs_file_sync_callback_fn sync = nullptr;
	duckdb_v2_vfs_file_truncate_callback_fn truncate = nullptr;
	duckdb_v2_vfs_stat_callback_fn stat_path = nullptr;
	duckdb_v2_vfs_list_callback_fn list = nullptr;
	duckdb_v2_vfs_glob_callback_fn glob = nullptr;
	duckdb_v2_vfs_remove_file_callback_fn remove_file = nullptr;
	duckdb_v2_vfs_create_directory_callback_fn create_directory = nullptr;
	duckdb_v2_vfs_remove_directory_callback_fn remove_directory = nullptr;
	duckdb_v2_vfs_move_callback_fn move = nullptr;
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

// What every callback receives about the operation it serves: the query's context and opener when there is one,
// and the way back to the file system.
class CV2VirtualFileSystemInfo {
public:
	optional_ptr<ClientContext> context;
	optional_ptr<FileOpener> opener;
	optional_ptr<CV2VirtualFileSystem> owner;
	//! Whether this is the open callback's info, which the vfs_file_open functions are valid on.
	bool is_open = false;
};

// An open file. The position is the engine's cursor, used only when the file system does not own one.
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
	//! What the file callbacks receive. Without a per-call context from the engine it is fixed for the file's life.
	CV2VirtualFileSystemInfo op_info;
};

// The open callback's info: the operation plus the open request it answers.
class CV2VirtualFileOpenInfo : public CV2VirtualFileSystemInfo {
public:
	const OpenFileInfo *file = nullptr;
	FileOpenFlags flags;
	//! Owned as soon as it is attached, so it is destroyed on every path out of the open.
	CV2UserData data;
	bool is_seekable = true;
	bool is_on_disk = false;
	//! What a listing reported about the file, decoded from the open options on first request.
	unique_ptr<CV2FileMetadata> listed_metadata;
};

static auto Convert(duckdb_v2_vfs_info_handle info) -> CV2VirtualFileSystemInfo * {
	return reinterpret_cast<CV2VirtualFileSystemInfo *>(info);
}
static auto Convert(CV2VirtualFileSystemInfo *info) -> duckdb_v2_vfs_info_handle {
	return reinterpret_cast<duckdb_v2_vfs_info_handle>(info);
}

// The open callback's info behind a handle, for the functions only valid there.
static CV2VirtualFileOpenInfo &OpenInfoOf(duckdb_v2_vfs_info_handle info, const char *function) {
	auto &base = *Convert(info);
	if (!base.is_open) {
		throw InvalidInputException("%s is only valid in the file open callback", function);
	}
	return static_cast<CV2VirtualFileOpenInfo &>(base);
}

static vector<DUCKDB_V2_FILE_FLAG> FlagList(const FileOpenFlags &flags);

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

// The engine-side file system: routes every engine call to the matching callback, and fills in what the callbacks
// leave out (the engine's cursor, plain-path globs, directory globs over listings, try-removal).
class CV2VirtualFileSystem final : public FileSystem {
public:
	CV2VirtualFileSystem(CV2VirtualFileSystemConfig config_p, DatabaseInstance &db)
	    : config(std::move(config_p)), db(db) {
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
		if (!config.callbacks.claim) {
			return false;
		}
		bool result = false;
		auto info = SystemInfo(nullptr);
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			config.callbacks.claim(Convert(&info), Convert(path), &result, &err);
		});
		return result;
	}

	void *UserData() const {
		return config.user_data ? config.user_data->GetData() : nullptr;
	}

	//! The database's file system as a borrowed consumer handle, for callbacks that delegate without a context. The
	//! engine's wrapper pushes the database's own opener, so requests through it see database-level settings and
	//! secrets.
	CV2FileSystem &DelegateSlot() {
		lock_guard<mutex> guard(delegate_lock);
		if (!delegate) {
			delegate = make_shared_ptr<CV2FileSystem>();
			delegate->fs = &db.GetFileSystem();
		}
		return *delegate;
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
		static_cast<CV2VirtualFileSystemInfo &>(info) = SystemInfo(opener);
		info.is_open = true;
		info.file = &file;
		info.flags = flags;
		auto flag_list = FlagList(flags);

		auto err = TryInvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.open(Convert(&info), Convert(file.path), flag_list.data(), flag_list.size(), &err);
		});
		if (err.HasError()) {
			// Whatever the callback attached is destroyed with `info`.
			if (err.code == DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND && flags.ReturnNullIfNotExists()) {
				return nullptr;
			}
			err.ThrowAsException();
		}
		if (!info.data.GetData()) {
			// The state is all a file callback receives about the file, so an open without it is a bug.
			throw InvalidInputException("The open callback of file system \"%s\" returned without attaching file data "
			                            "for \"%s\"",
			                            config.name, file.path);
		}

		if (!info.is_seekable && !config.OwnsCursor()) {
			// The engine's cursor is a position in the file, which a stream does not have. `info` releases the state.
			throw NotImplementedException("File system \"%s\" reports \"%s\" as not seekable, which only a file "
			                              "system that owns the cursor can serve",
			                              config.name, file.path);
		}
		auto handle = make_uniq<CV2VirtualFile>(*this, file.path, flags, std::move(info.data));
		handle->op_info = SystemInfo(nullptr);
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
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			config.callbacks.close(Convert(&handle.op_info), handle.Data(), &err);
		});
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
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			config.callbacks.abort(Convert(&handle.op_info), handle.Data(), &err);
		});
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
			cb.write_at(Convert(&handle.op_info), handle.Data(), buffer, NumericCast<idx_t>(nr_bytes), location, &err);
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
			cb.read(Convert(&handle.op_info), handle.Data(), buffer, count, &bytes_read, &err);
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
			cb.write(Convert(&handle.op_info), handle.Data(), buffer, NumericCast<idx_t>(nr_bytes), &err);
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
		    [&](duckdb_v2_error_info_handle err) { cb.seek(Convert(&handle.op_info), handle.Data(), location, &err); });
	}

	idx_t SeekPosition(FileHandle &handle_p) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		auto &cb = config.callbacks;
		if (!config.OwnsCursor()) {
			return handle.position;
		}
		RequireCallback(cb.tell, "tell");
		idx_t position = 0;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.tell(Convert(&handle.op_info), handle.Data(), &position, &err);
		});
		return position;
	}

	int64_t GetFileSize(FileHandle &handle) override {
		auto metadata = Stat(handle.Cast<CV2VirtualFile>());
		if (!metadata.size) {
			throw IOException("The size of \"%s\" is not known to file system \"%s\", and this operation needs it",
			                  handle.path, config.name);
		}
		return NumericCast<int64_t>(*metadata.size);
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return Stat(handle.Cast<CV2VirtualFile>()).LastModified();
	}

	string GetVersionTag(FileHandle &handle) override {
		auto metadata = Stat(handle.Cast<CV2VirtualFile>());
		return metadata.version_tag ? *metadata.version_tag : string();
	}

	static FileType ToFileType(DUCKDB_V2_FILE_TYPE type) {
		// An open file is a regular file unless the stat callback says it is a pipe.
		return type == DUCKDB_V2_FILE_TYPE_PIPE ? FileType::FILE_TYPE_FIFO : FileType::FILE_TYPE_REGULAR;
	}

	FileType GetFileType(FileHandle &handle) override {
		return ToFileType(Stat(handle.Cast<CV2VirtualFile>()).type);
	}

	FileMetadata Stats(FileHandle &handle) override {
		auto metadata = Stat(handle.Cast<CV2VirtualFile>());
		FileMetadata result;
		result.file_size = metadata.size ? NumericCast<int64_t>(*metadata.size) : -1;
		result.last_modification_time = metadata.LastModified();
		result.file_type = ToFileType(metadata.type);
		if (metadata.version_tag) {
			result.version_tag = *metadata.version_tag;
		}
		return result;
	}

	void FileSync(FileHandle &handle_p) override {
		if (!config.callbacks.sync) {
			return;
		}
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			config.callbacks.sync(Convert(&handle.op_info), handle.Data(), &err);
		});
	}

	void Truncate(FileHandle &handle_p, int64_t new_size) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		auto &cb = config.callbacks;
		RequireCallback(cb.truncate, "truncate");
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.truncate(Convert(&handle.op_info), handle.Data(), NumericCast<idx_t>(new_size), &err);
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
	optional<FileMetadata> GetStatsIfExists(const OpenFileInfo &file, optional_ptr<FileOpener> opener) override {
		if (!config.callbacks.stat_path) {
			// The engine's own way: open the file and ask it.
			return FileSystem::GetStatsIfExists(file, opener);
		}
		auto metadata = StatPath(file.path, opener);
		if (metadata.type == DUCKDB_V2_FILE_TYPE_INVALID) {
			return nullopt;
		}
		FileMetadata result;
		result.file_size = metadata.size ? NumericCast<int64_t>(*metadata.size) : -1;
		result.last_modification_time = metadata.LastModified();
		result.file_type = CV2FileMetadata::ToEngineType(metadata.type);
		if (metadata.version_tag) {
			result.version_tag = *metadata.version_tag;
		}
		return result;
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override {
		return StatPath(filename, opener).type == DUCKDB_V2_FILE_TYPE_REGULAR;
	}

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override {
		return StatPath(directory, opener).type == DUCKDB_V2_FILE_TYPE_DIRECTORY;
	}

	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override {
		// Asked on every open; a file system that cannot metadata paths has no pipes to report.
		if (!config.callbacks.stat_path) {
			return false;
		}
		return StatPath(filename, opener).type == DUCKDB_V2_FILE_TYPE_PIPE;
	}

	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		RequireCallback(cb.remove_file, "remove file");
		auto path = SystemInfo(opener);
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { cb.remove_file(Convert(&path), Convert(filename), &err); });
	}

	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		RequireCallback(cb.remove_file, "remove file");
		auto path = SystemInfo(opener);
		auto err = TryInvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { cb.remove_file(Convert(&path), Convert(filename), &err); });
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
		auto path = SystemInfo(opener);
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { cb.create_directory(Convert(&path), Convert(directory), &err); });
		return true;
	}

	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) override {
		RemoveDirectoryExtended(directory, {RemoveDirectoryMode::RECURSIVE}, opener);
	}

	bool RemoveDirectoryExtended(const string &directory, const RemoveDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		RequireCallback(cb.remove_directory, "remove directory");
		auto path = SystemInfo(opener);
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { cb.remove_directory(Convert(&path), Convert(directory), &err); });
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
		auto path = SystemInfo(opener);
		InvokeCallback(
		    [&](duckdb_v2_error_info_handle err) { cb.move(Convert(&path), Convert(source), Convert(target), &err); });
	}

protected:
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
			auto pattern = SystemInfo(opener);
			InvokeCallback([&](duckdb_v2_error_info_handle err) {
				cb.glob(Convert(&pattern), Convert(path), Convert(&listing), &err);
			});
			for (auto &entry : listing.entries) {
				result.push_back(ToOpenFileInfo(entry));
			}
		} else if (!HasGlob(path)) {
			// A plain path names one file. Without a stat callback there is no way to check, so let the open fail.
			if (!cb.stat_path || FileExists(path, opener)) {
				result.emplace_back(path);
			}
		} else {
			throw NotImplementedException("File system \"%s\" has no glob callback, so the pattern \"%s\" cannot be "
			                              "expanded",
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

	CV2VirtualFileSystemInfo SystemInfo(optional_ptr<FileOpener> opener) {
		CV2VirtualFileSystemInfo info;
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
		entry.metadata.FillOptions(options);
		return info;
	}

	idx_t ReadAt(CV2VirtualFile &handle, void *buffer, idx_t count, idx_t location) {
		auto &cb = config.callbacks;
		RequireCallback(cb.read_at, "read at");
		idx_t bytes_read = 0;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.read_at(Convert(&handle.op_info), handle.Data(), buffer, count, location, &bytes_read, &err);
		});
		if (bytes_read > count) {
			throw IOException("The read at callback of file system \"%s\" reported %llu bytes read into a buffer "
			                  "of %llu",
			                  config.name, bytes_read, count);
		}
		return bytes_read;
	}

	CV2FileMetadata Stat(CV2VirtualFile &handle) {
		auto &cb = config.callbacks;
		RequireCallback(cb.stat, "stat");
		CV2FileMetadata info;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.stat(Convert(&handle.op_info), handle.Data(), Convert(&info), &err);
		});
		return info;
	}

	CV2FileMetadata StatPath(const string &path_p, optional_ptr<FileOpener> opener) {
		auto &cb = config.callbacks;
		RequireCallback(cb.stat_path, "stat");
		auto path = SystemInfo(opener);
		CV2FileMetadata info;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.stat_path(Convert(&path), Convert(path_p), Convert(&info), &err);
		});
		return info;
	}

	CV2FileListing List(const string &directory, optional_ptr<FileOpener> opener) {
		auto &cb = config.callbacks;
		RequireCallback(cb.list, "list");
		auto path = SystemInfo(opener);
		CV2FileListing listing;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.list(Convert(&path), Convert(directory), Convert(&listing), &err);
		});
		return listing;
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
		if (config.prefixes.empty() && !cb.claim) {
			throw InvalidInputException("A prefix or a claim callback must be set for the file system.");
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
			throw InvalidInputException("Stat callback for open files must be set for the file system.");
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

static auto Convert(duckdb_v2_vfs_handle fs) -> CV2VirtualFileSystemBuilder * {
	return reinterpret_cast<CV2VirtualFileSystemBuilder *>(fs);
}
static auto Convert(CV2VirtualFileSystemBuilder *fs) -> duckdb_v2_vfs_handle {
	return reinterpret_cast<duckdb_v2_vfs_handle>(fs);
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

static bool HasFlag(FileOpenFlags flags, DUCKDB_V2_FILE_FLAG flag) {
	switch (flag) {
	case DUCKDB_V2_FILE_FLAG_SHARED_LOCK:
		return flags.Lock() == FileLockType::READ_LOCK;
	case DUCKDB_V2_FILE_FLAG_EXCLUSIVE_LOCK:
		return flags.Lock() == FileLockType::WRITE_LOCK;
	default:
		return (flags.GetFlagsInternal() & FileFlagBit(flag)) != 0;
	}
}

// Every flag that is set, in enum order.
static vector<DUCKDB_V2_FILE_FLAG> FlagList(const FileOpenFlags &flags) {
	static constexpr DUCKDB_V2_FILE_FLAG ALL_FLAGS[] = {DUCKDB_V2_FILE_FLAG_READ,
	                                                    DUCKDB_V2_FILE_FLAG_WRITE,
	                                                    DUCKDB_V2_FILE_FLAG_CREATE,
	                                                    DUCKDB_V2_FILE_FLAG_CREATE_NEW,
	                                                    DUCKDB_V2_FILE_FLAG_APPEND,
	                                                    DUCKDB_V2_FILE_FLAG_EXCLUSIVE_CREATE,
	                                                    DUCKDB_V2_FILE_FLAG_PARALLEL_ACCESS,
	                                                    DUCKDB_V2_FILE_FLAG_SHARED_LOCK,
	                                                    DUCKDB_V2_FILE_FLAG_EXCLUSIVE_LOCK};
	vector<DUCKDB_V2_FILE_FLAG> result;
	for (auto flag : ALL_FLAGS) {
		if (HasFlag(flags, flag)) {
			result.push_back(flag);
		}
	}
	return result;
}

static auto ContextHandle(optional_ptr<ClientContext> context) -> duckdb_v2_context_handle {
	return context ? Convert(context.get()) : nullptr;
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_vfs_create_with_connection(duckdb_v2_connection_handle connection,
                                                     duckdb_v2_vfs_handle *file_system,
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

DUCKDB_V2_ERROR duckdb_v2_vfs_create_with_extension(duckdb_v2_extension_handle extension,
                                                    duckdb_v2_vfs_handle *file_system,
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

DUCKDB_V2_ERROR duckdb_v2_vfs_set_name(duckdb_v2_vfs_handle file_system, duckdb_v2_str name,
                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(name);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.name = duckdb::string(Convert(name)); });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_add_prefix(duckdb_v2_vfs_handle file_system, duckdb_v2_str prefix,
                                         duckdb_v2_error_info_handle *err) {
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

DUCKDB_V2_ERROR duckdb_v2_vfs_set_user_data(duckdb_v2_vfs_handle file_system, duckdb_v2_opaque *data,
                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(file_system)->config.user_data =
		    duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_claim_callback(duckdb_v2_vfs_handle file_system, duckdb_v2_vfs_claim_callback_fn callback,
                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.claim = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_set_file_open_callback(duckdb_v2_vfs_handle file_system,
                                                     duckdb_v2_vfs_file_open_callback_fn callback,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.open = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_file_close_callback(duckdb_v2_vfs_handle file_system, duckdb_v2_vfs_file_close_callback_fn callback,
                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.close = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_file_abort_callback(duckdb_v2_vfs_handle file_system, duckdb_v2_vfs_file_abort_callback_fn callback,
                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.abort = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_file_read_at_callback(duckdb_v2_vfs_handle file_system,
                                        duckdb_v2_vfs_file_read_at_callback_fn callback,
                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.read_at = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_file_write_at_callback(duckdb_v2_vfs_handle file_system,
                                         duckdb_v2_vfs_file_write_at_callback_fn callback,
                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.write_at = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_file_read_callback(duckdb_v2_vfs_handle file_system, duckdb_v2_vfs_file_read_callback_fn callback,
                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.read = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_file_write_callback(duckdb_v2_vfs_handle file_system, duckdb_v2_vfs_file_write_callback_fn callback,
                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.write = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_file_seek_callback(duckdb_v2_vfs_handle file_system, duckdb_v2_vfs_file_seek_callback_fn callback,
                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.seek = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_file_tell_callback(duckdb_v2_vfs_handle file_system, duckdb_v2_vfs_file_tell_callback_fn callback,
                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.tell = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_file_stat_callback(duckdb_v2_vfs_handle file_system, duckdb_v2_vfs_file_stat_callback_fn callback,
                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.stat = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_file_sync_callback(duckdb_v2_vfs_handle file_system, duckdb_v2_vfs_file_sync_callback_fn callback,
                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.sync = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_file_truncate_callback(duckdb_v2_vfs_handle file_system,
                                         duckdb_v2_vfs_file_truncate_callback_fn callback,
                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.truncate = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_stat_callback(duckdb_v2_vfs_handle file_system, duckdb_v2_vfs_stat_callback_fn callback,
                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.stat_path = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_set_list_callback(duckdb_v2_vfs_handle file_system,
                                                duckdb_v2_vfs_list_callback_fn callback,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.list = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_set_glob_callback(duckdb_v2_vfs_handle file_system,
                                                duckdb_v2_vfs_glob_callback_fn callback,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.glob = callback; });
}

DUCKDB_V2_ERROR
duckdb_v2_vfs_set_remove_file_callback(duckdb_v2_vfs_handle file_system, duckdb_v2_vfs_remove_file_callback_fn callback,
                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.remove_file = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_set_create_directory_callback(duckdb_v2_vfs_handle file_system,
                                                            duckdb_v2_vfs_create_directory_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.create_directory = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_set_remove_directory_callback(duckdb_v2_vfs_handle file_system,
                                                            duckdb_v2_vfs_remove_directory_callback_fn callback,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.remove_directory = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_set_move_callback(duckdb_v2_vfs_handle file_system,
                                                duckdb_v2_vfs_move_callback_fn callback,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->config.callbacks.move = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_file_open_get_value(duckdb_v2_vfs_info_handle info, duckdb_v2_str name,
                                                  duckdb_v2_value_handle *value, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(value);
	*value = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &open_info = OpenInfoOf(info, "duckdb_v2_vfs_file_open_get_value");
		auto &extended_info = open_info.file->extended_info;
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

DUCKDB_V2_ERROR duckdb_v2_vfs_file_open_get_metadata(duckdb_v2_vfs_info_handle info,
                                                     duckdb_v2_file_metadata_handle *metadata,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(metadata);
	*metadata = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &open_info = OpenInfoOf(info, "duckdb_v2_vfs_file_open_get_metadata");
		if (!open_info.listed_metadata) {
			open_info.listed_metadata =
			    duckdb::make_uniq<CV2FileMetadata>(CV2FileMetadata::FromOptions(*open_info.file));
		}
		*metadata = Convert(open_info.listed_metadata.get());
	});
}

DUCKDB_V2_ERROR duckdb_v2_vfs_file_open_get_options(duckdb_v2_vfs_info_handle info,
                                                    duckdb_v2_file_open_options_handle *options,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(options);
	*options = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &open_info = OpenInfoOf(info, "duckdb_v2_vfs_file_open_get_options");
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

DUCKDB_V2_ERROR duckdb_v2_vfs_file_open_set_data(duckdb_v2_vfs_info_handle info, duckdb_v2_opaque *data,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		// Assigning destroys whatever was attached before.
		OpenInfoOf(info, "duckdb_v2_vfs_file_open_set_data").data = CV2UserData(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vfs_file_open_set_property(duckdb_v2_vfs_info_handle info, DUCKDB_V2_FILE_PROPERTY property,
                                                     bool value, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		auto &open_info = OpenInfoOf(info, "duckdb_v2_vfs_file_open_set_property");
		switch (property) {
		case DUCKDB_V2_FILE_PROPERTY_IS_SEEKABLE:
			open_info.is_seekable = value;
			break;
		case DUCKDB_V2_FILE_PROPERTY_IS_ON_DISK:
			open_info.is_on_disk = value;
			break;
		default:
			throw duckdb::InvalidInputException("'%d' is not a file property.", static_cast<int>(property));
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_vfs_info_get_user_data(duckdb_v2_vfs_info_handle info, void **data,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(info)->owner->UserData(); });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_info_get_file_system(duckdb_v2_vfs_info_handle info,
                                                   duckdb_v2_file_system_handle *file_system,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(file_system);
	*file_system = nullptr;
	return WithErrorHandler(err, [&]() { *file_system = Convert(&Convert(info)->owner->DelegateSlot()); });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_info_try_get_context(duckdb_v2_vfs_info_handle info, duckdb_v2_context_handle *context,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(context);
	*context = nullptr;
	return WithErrorHandler(err, [&]() { *context = ContextHandle(Convert(info)->context); });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_register(duckdb_v2_vfs_handle file_system, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	return WithErrorHandler(err, [&]() { Convert(file_system)->Register(); });
}

DUCKDB_V2_ERROR duckdb_v2_vfs_destroy(duckdb_v2_vfs_handle *file_system) {
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
