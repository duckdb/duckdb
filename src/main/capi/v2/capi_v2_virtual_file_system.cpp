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
};

// The per-operation handle of every callback but open. Nothing in it yet, so one shared instance serves them all.
struct CV2VirtualFileOperationInfo {};

template <class HANDLE>
static HANDLE EmptyOperationInfo() {
	static CV2VirtualFileOperationInfo info;
	return reinterpret_cast<HANDLE>(&info);
}

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
	//! What was known about the file when it was opened. Kept only for a file system without a stat callback, which
	//! it answers for.
	FileMetadata metadata;
	//! Whether the file was written or truncated since, which makes that metadata stale.
	std::atomic<bool> written {false};
	//! What the file callbacks receive. Without a per-call context from the engine it is fixed for the file's life.
	CV2VirtualFileSystemInfo op_info;
};

// The open callback's own handle: the open request it answers.
class CV2VirtualFileOpenInfo {
public:
	//! Owned as soon as it is attached, so it is destroyed on every path out of the open.
	CV2UserData data;
	bool is_seekable = true;
	bool is_on_disk = false;
};

static auto Convert(duckdb_v2_vfs_info_handle info) -> CV2VirtualFileSystemInfo * {
	return reinterpret_cast<CV2VirtualFileSystemInfo *>(info);
}
static auto Convert(CV2VirtualFileSystemInfo *info) -> duckdb_v2_vfs_info_handle {
	return reinterpret_cast<duckdb_v2_vfs_info_handle>(info);
}
static auto Convert(duckdb_v2_vfs_file_open_info_handle info) -> CV2VirtualFileOpenInfo * {
	return reinterpret_cast<CV2VirtualFileOpenInfo *>(info);
}
static auto Convert(CV2VirtualFileOpenInfo *info) -> duckdb_v2_vfs_file_open_info_handle {
	return reinterpret_cast<duckdb_v2_vfs_file_open_info_handle>(info);
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
			config.callbacks.claim(Convert(&info), EmptyOperationInfo<duckdb_v2_vfs_claim_info_handle>(), Convert(path),
			                       &result, &err);
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
protected:
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
			if (config.OwnsCursor() && !cb.write) {
				throw NotImplementedException("File system \"%s\" owns the cursor but has no \"write\" callback, which "
				                              "\"%s\" needs since it is opened for writing",
				                              config.name, file.path);
			}
		}
		if (flags.OpenForReading() && config.OwnsCursor() && !cb.read) {
			throw NotImplementedException("File system \"%s\" owns the cursor but has no \"read\" callback, which "
			                              "\"%s\" needs since it is opened for reading",
			                              config.name, file.path);
		}
		if (flags.OpenForReading() && !config.CanRead()) {
			throw PermissionException("File system \"%s\" is write-only: it has no \"read at\" callback, so \"%s\" "
			                          "cannot be opened for reading",
			                          config.name, file.path);
		}

		CV2VirtualFileOpenInfo info;
		auto system_info = SystemInfo(opener);
		auto flag_list = FlagList(flags);
		// What a listing knew about the file and the caller's hints, for the callback to read and fill in.
		auto metadata = CV2FileMetadata::FromOptions(file);

		auto err = TryInvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.open(Convert(&system_info), Convert(&info), Convert(file.path), flag_list.data(), flag_list.size(),
			        Convert(&metadata), &err);
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
		if (!cb.stat) {
			handle->metadata = std::move(metadata.data);
		}
		if (flags.OpenForAppending() && !config.OwnsCursor()) {
			// The engine's cursor starts at the end; the file system's own cursor is its business.
			handle->position = NumericCast<idx_t>(GetFileSize(*handle));
		}
		return std::move(handle);
	}

public:
	static bool OpenedForWriting(const CV2VirtualFile &handle) {
		return handle.flags.OpenForWriting() || handle.flags.OpenForAppending();
	}

	void CloseFile(CV2VirtualFile &handle) {
		if (!config.callbacks.close) {
			return;
		}
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			config.callbacks.close(Convert(&handle.op_info), EmptyOperationInfo<duckdb_v2_vfs_file_close_info_handle>(),
			                       handle.Data(), &err);
		});
	}

	bool CanAbort() const {
		return config.callbacks.abort != nullptr;
	}

	void AbortFileWrite(FileHandle &handle_p) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		if (!CanAbort()) {
			handle.Close();
			return;
		}
		if (handle.closed) {
			return;
		}
		// Abort takes the place of close, so the close callback never sees an aborted file.
		handle.closed = true;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			config.callbacks.abort(Convert(&handle.op_info), EmptyOperationInfo<duckdb_v2_vfs_file_abort_info_handle>(),
			                       handle.Data(), &err);
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
		handle.written = true;
		auto total = NumericCast<idx_t>(nr_bytes);
		auto *data = static_cast<const_data_ptr_t>(buffer);
		// The callback may come up short; the engine's contract here is all or nothing.
		idx_t done = 0;
		while (done < total) {
			idx_t written = 0;
			InvokeCallback([&](duckdb_v2_error_info_handle err) {
				cb.write_at(Convert(&handle.op_info), EmptyOperationInfo<duckdb_v2_vfs_file_write_at_info_handle>(),
				            handle.Data(), data + done, total - done, location + done, &written, &err);
			});
			CheckWritten(handle, "write at", written, total, done);
			done += written;
		}
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
			cb.read(Convert(&handle.op_info), EmptyOperationInfo<duckdb_v2_vfs_file_read_info_handle>(), handle.Data(),
			        buffer, count, &bytes_read, &err);
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
		handle.written = true;
		auto total = NumericCast<idx_t>(nr_bytes);
		auto *data = static_cast<const_data_ptr_t>(buffer);
		// The callback may come up short; the engine's writers expect everything to be written.
		idx_t done = 0;
		while (done < total) {
			idx_t written = 0;
			InvokeCallback([&](duckdb_v2_error_info_handle err) {
				cb.write(Convert(&handle.op_info), EmptyOperationInfo<duckdb_v2_vfs_file_write_info_handle>(),
				         handle.Data(), data + done, total - done, &written, &err);
			});
			CheckWritten(handle, "write", written, total, done);
			done += written;
		}
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
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.seek(Convert(&handle.op_info), EmptyOperationInfo<duckdb_v2_vfs_file_seek_info_handle>(), handle.Data(),
			        location, &err);
		});
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
			cb.tell(Convert(&handle.op_info), EmptyOperationInfo<duckdb_v2_vfs_file_tell_info_handle>(), handle.Data(),
			        &position, &err);
		});
		return position;
	}

	int64_t GetFileSize(FileHandle &handle) override {
		auto metadata = Stat(handle.Cast<CV2VirtualFile>());
		if (metadata.file_size < 0) {
			throw IOException("The size of \"%s\" is not known to file system \"%s\", and this operation needs it",
			                  handle.path, config.name);
		}
		return metadata.file_size;
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return Stat(handle.Cast<CV2VirtualFile>()).last_modification_time;
	}

	string GetVersionTag(FileHandle &handle) override {
		return Stat(handle.Cast<CV2VirtualFile>()).version_tag;
	}

	FileType GetFileType(FileHandle &handle) override {
		return Stat(handle.Cast<CV2VirtualFile>()).file_type;
	}

	FileMetadata Stats(FileHandle &handle) override {
		return Stat(handle.Cast<CV2VirtualFile>());
	}

	void FileSync(FileHandle &handle_p) override {
		if (!config.callbacks.sync) {
			return;
		}
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			config.callbacks.sync(Convert(&handle.op_info), EmptyOperationInfo<duckdb_v2_vfs_file_sync_info_handle>(),
			                      handle.Data(), &err);
		});
	}

	void Truncate(FileHandle &handle_p, int64_t new_size) override {
		auto &handle = handle_p.Cast<CV2VirtualFile>();
		auto &cb = config.callbacks;
		RequireCallback(cb.truncate, "truncate");
		handle.written = true;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.truncate(Convert(&handle.op_info), EmptyOperationInfo<duckdb_v2_vfs_file_truncate_info_handle>(),
			            handle.Data(), NumericCast<idx_t>(new_size), &err);
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
		return StatPath(file.path, opener);
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override {
		if (!config.callbacks.stat_path) {
			// Open it to find out; a file system that cannot read has no way to tell.
			if (!config.CanRead()) {
				return false;
			}
			auto stats = FileSystem::GetStatsIfExists(OpenFileInfo(filename), opener);
			return stats && stats->file_type == FileType::FILE_TYPE_REGULAR;
		}
		return HasType(StatPath(filename, opener), FileType::FILE_TYPE_REGULAR);
	}

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override {
		if (!config.callbacks.stat_path) {
			// A directory with something in it exists. An empty listing proves nothing, since a backend without
			// directories lists a missing one as empty.
			CV2FileListing listing;
			return config.callbacks.list && TryList(directory, opener, listing) && !listing.entries.empty();
		}
		return HasType(StatPath(directory, opener), FileType::FILE_TYPE_DIR);
	}

	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override {
		// Asked on every open; a file system that cannot metadata paths has no pipes to report.
		if (!config.callbacks.stat_path) {
			return false;
		}
		return HasType(StatPath(filename, opener), FileType::FILE_TYPE_FIFO);
	}

	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		RequireCallback(cb.remove_file, "remove file");
		auto path = SystemInfo(opener);
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.remove_file(Convert(&path), EmptyOperationInfo<duckdb_v2_vfs_remove_file_info_handle>(),
			               Convert(filename), &err);
		});
	}

	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		auto &cb = config.callbacks;
		RequireCallback(cb.remove_file, "remove file");
		auto path = SystemInfo(opener);
		auto err = TryInvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.remove_file(Convert(&path), EmptyOperationInfo<duckdb_v2_vfs_remove_file_info_handle>(),
			               Convert(filename), &err);
		});
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
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.create_directory(Convert(&path), EmptyOperationInfo<duckdb_v2_vfs_create_directory_info_handle>(),
			                    Convert(directory), &err);
		});
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
		auto err = TryInvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.remove_directory(Convert(&path), EmptyOperationInfo<duckdb_v2_vfs_remove_directory_info_handle>(),
			                    Convert(directory), &err);
		});
		if (err.HasError()) {
			// The engine's way of saying that the directory was not there.
			if (err.code == DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND) {
				return false;
			}
			err.ThrowAsException();
		}
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
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.move(Convert(&path), EmptyOperationInfo<duckdb_v2_vfs_move_info_handle>(), Convert(source),
			        Convert(target), &err);
		});
	}

protected:
	bool SupportsListFilesExtended() const override {
		return true;
	}

	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override {
		CV2FileListing listing;
		if (!TryList(directory, opener, listing)) {
			return false;
		}
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
				cb.glob(Convert(&pattern), EmptyOperationInfo<duckdb_v2_vfs_glob_info_handle>(), Convert(path),
				        Convert(&listing), &err);
			});
			for (auto &entry : listing.entries) {
				result.push_back(ToOpenFileInfo(entry));
			}
		} else if (!cb.stat_path || FileExists(path, opener)) {
			// Globbing is opt-in: without the callback every path names one file, whatever characters it holds.
			// Without a stat callback there is no way to check that it exists, so let the open fail.
			result.emplace_back(path);
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
		entry.metadata.FillOptions(*info.extended_info);
		return info;
	}

	//! The engine's writers need every byte, so a write that stops making progress fails.
	void CheckWritten(CV2VirtualFile &handle, const char *what, idx_t written, idx_t total, idx_t done) const {
		if (written > total - done) {
			throw IOException("The %s callback of file system \"%s\" reported %llu bytes written from a buffer of "
			                  "%llu",
			                  what, config.name, written, total - done);
		}
		if (written == 0) {
			throw IOException("Could not write all bytes to file \"%s\": wanted %llu bytes, but only %llu could be "
			                  "written",
			                  handle.path, total, done);
		}
	}

	idx_t ReadAt(CV2VirtualFile &handle, void *buffer, idx_t count, idx_t location) {
		auto &cb = config.callbacks;
		RequireCallback(cb.read_at, "read at");
		idx_t bytes_read = 0;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.read_at(Convert(&handle.op_info), EmptyOperationInfo<duckdb_v2_vfs_file_read_at_info_handle>(),
			           handle.Data(), buffer, count, location, &bytes_read, &err);
		});
		if (bytes_read > count) {
			throw IOException("The read at callback of file system \"%s\" reported %llu bytes read into a buffer "
			                  "of %llu",
			                  config.name, bytes_read, count);
		}
		return bytes_read;
	}

	//! What is known about an open file, which is a regular file unless it was said to be a pipe.
	FileMetadata Stat(CV2VirtualFile &handle) {
		auto &cb = config.callbacks;
		CV2FileMetadata info;
		if (cb.stat) {
			InvokeCallback([&](duckdb_v2_error_info_handle err) {
				cb.stat(Convert(&handle.op_info), EmptyOperationInfo<duckdb_v2_vfs_file_stat_info_handle>(),
				        handle.Data(), Convert(&info), &err);
			});
		} else {
			info.data = handle.metadata;
			if (handle.written) {
				// Writes have moved the file on from what the open knew.
				info.data.file_size = -1;
				info.data.last_modification_time = timestamp_t::ninfinity();
				info.data.version_tag.clear();
			}
		}
		if (info.data.file_type != FileType::FILE_TYPE_FIFO) {
			info.data.file_type = FileType::FILE_TYPE_REGULAR;
		}
		return std::move(info.data);
	}

	static bool HasType(const optional<FileMetadata> &metadata, FileType type) {
		return metadata && metadata->file_type == type;
	}

	//! What the stat callback reports about a path, or nothing for a path that does not exist.
	optional<FileMetadata> StatPath(const string &path_p, optional_ptr<FileOpener> opener) {
		auto &cb = config.callbacks;
		RequireCallback(cb.stat_path, "stat");
		auto path = SystemInfo(opener);
		CV2FileMetadata info;
		bool exists = false;
		InvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.stat_path(Convert(&path), EmptyOperationInfo<duckdb_v2_vfs_stat_info_handle>(), Convert(path_p),
			             Convert(&info), &exists, &err);
		});
		if (!exists) {
			if (!info.IsEmpty()) {
				throw InvalidInputException("The stat callback of file system \"%s\" described \"%s\" without "
				                            "reporting that it exists",
				                            config.name, path_p);
			}
			return nullopt;
		}
		if (!info.HasType()) {
			info.data.file_type = FileType::FILE_TYPE_REGULAR;
		}
		return std::move(info.data);
	}

	//! Fills the listing, or reports false for a directory the file system says does not exist.
	bool TryList(const string &directory, optional_ptr<FileOpener> opener, CV2FileListing &listing) {
		auto &cb = config.callbacks;
		RequireCallback(cb.list, "list");
		auto path = SystemInfo(opener);
		auto err = TryInvokeCallback([&](duckdb_v2_error_info_handle err) {
			cb.list(Convert(&path), EmptyOperationInfo<duckdb_v2_vfs_list_info_handle>(), Convert(directory),
			        Convert(&listing), &err);
		});
		if (!err.HasError()) {
			return true;
		}
		if (err.code == DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND) {
			return false;
		}
		err.ThrowAsException();
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
		// Owning the cursor means saying where it is; what else it takes depends on how a file is opened.
		if (config.OwnsCursor() && !cb.tell) {
			throw InvalidInputException("Tell callback must be set for a file system that owns the cursor by "
			                            "setting any of the read, write, seek or tell callbacks.");
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

DUCKDB_V2_ERROR duckdb_v2_vfs_file_open_set_data(duckdb_v2_vfs_file_open_info_handle info, duckdb_v2_opaque *data,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		// Assigning destroys whatever was attached before.
		Convert(info)->data = CV2UserData(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_vfs_file_open_set_property(duckdb_v2_vfs_file_open_info_handle info,
                                                     DUCKDB_V2_FILE_PROPERTY property, bool value,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(info);
	return WithErrorHandler(err, [&]() {
		auto &open_info = *Convert(info);
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
