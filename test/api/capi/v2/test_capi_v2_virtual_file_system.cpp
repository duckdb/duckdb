#include "test_capi_v2.hpp"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 virtual file system tests: register an in-memory file system under the
// "mem://" scheme and drive it through SQL (read_csv, glob, COPY TO) and
// through the C file API.
//
// Callbacks avoid Catch assertions: a REQUIRE would throw through the C
// callback boundary into the engine. On failure they populate the provided
// error slot and return; the failure then surfaces as a query error that the
// test asserts on. Cross-callback observations are latched into the file
// system's own state and asserted after the query.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

constexpr const char *SCHEME = "mem://";

// The backing store: full path -> contents, plus explicitly created directories. Directories also exist implicitly
// as prefixes of file paths, the way an object store behaves.
struct MemFs {
	std::mutex lock;
	std::map<std::string, std::string> files;
	std::set<std::string> dirs;

	std::atomic<int> opens {0};
	std::atomic<int> failed_opens {0};
	std::atomic<int> closes {0};
	std::atomic<int> aborts {0};
	std::atomic<int> file_data_destroyed {0};
	std::atomic<int> read_at_calls {0};
	std::atomic<int> write_at_calls {0};
	std::atomic<int> read_calls {0};
	std::atomic<int> write_calls {0};
	std::atomic<int> seek_calls {0};
	std::atomic<int> tell_calls {0};
	std::atomic<int> sync_calls {0};
	std::atomic<int> user_data_destroyed {0};

	// What the open callback reports for every file.
	bool seekable = true;
	bool on_disk = false;

	// Latched by the callbacks.
	std::string last_open_path;
	bool last_open_for_write = false;
	bool last_open_parallel = false;
	bool last_open_had_context = false;
	std::string last_open_value;
	std::map<std::string, idx_t> listed_sizes; // path -> size a listing reported at open
	DUCKDB_V2_ERROR target_path_rc = DUCKDB_V2_ERROR_NONE;

	bool IsDirectory(const std::string &path) {
		if (dirs.count(path)) {
			return true;
		}
		auto prefix = path + "/";
		auto it = files.lower_bound(prefix);
		return it != files.end() && it->first.compare(0, prefix.size(), prefix) == 0;
	}
};

struct MemHandle {
	MemFs *fs;
	std::string path;
	idx_t cursor = 0;
};

void VfsFail(duckdb_v2_error_info_handle *err, DUCKDB_V2_ERROR code, const std::string &message) {
	duckdb_v2_error_info_set_code(*err, code);
	duckdb_v2_error_info_set_text(*err, Convert(message));
}

MemFs &VfsOf(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_error_info_handle *err) {
	void *data = nullptr;
	duckdb_v2_virtual_file_system_info_get_user_data(info, &data, err);
	return *static_cast<MemFs *>(data);
}

MemHandle &VfsFile(duckdb_v2_virtual_file_info_handle file, duckdb_v2_error_info_handle *err) {
	void *data = nullptr;
	duckdb_v2_virtual_file_info_get_file_data(file, &data, err);
	return *static_cast<MemHandle *>(data);
}

std::string VfsPath(duckdb_v2_virtual_file_path_info_handle path, duckdb_v2_error_info_handle *err) {
	duckdb_v2_str view {nullptr, 0};
	duckdb_v2_virtual_file_path_info_get_path(path, &view, err);
	return Convert(view);
}

// ---------------------------------------------------------------------------
// Callbacks
// ---------------------------------------------------------------------------

void MemClaim(duckdb_v2_virtual_file_system_info_handle, duckdb_v2_str path, bool *claim,
              duckdb_v2_error_info_handle *) {
	auto p = Convert(path);
	*claim = p.rfind(SCHEME, 0) == 0;
}

void MemDestroyHandle(void *data) {
	auto handle = static_cast<MemHandle *>(data);
	handle->fs->file_data_destroyed++;
	delete handle;
}

void MemOpen(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_open_info_handle open_info,
             duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	duckdb_v2_str path_view {nullptr, 0};
	if (duckdb_v2_virtual_file_open_info_get_path(open_info, &path_view, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto path = Convert(path_view);

	bool write = false, append = false, create = false, create_new = false, exclusive = false, parallel = false;
	duckdb_v2_virtual_file_open_info_has_flag(open_info, DUCKDB_V2_FILE_FLAG_WRITE, &write, err);
	duckdb_v2_virtual_file_open_info_has_flag(open_info, DUCKDB_V2_FILE_FLAG_APPEND, &append, err);
	duckdb_v2_virtual_file_open_info_has_flag(open_info, DUCKDB_V2_FILE_FLAG_CREATE, &create, err);
	duckdb_v2_virtual_file_open_info_has_flag(open_info, DUCKDB_V2_FILE_FLAG_CREATE_NEW, &create_new, err);
	duckdb_v2_virtual_file_open_info_has_flag(open_info, DUCKDB_V2_FILE_FLAG_EXCLUSIVE_CREATE, &exclusive, err);
	duckdb_v2_virtual_file_open_info_has_flag(open_info, DUCKDB_V2_FILE_FLAG_PARALLEL_ACCESS, &parallel, err);

	duckdb_v2_value_handle value = nullptr;
	if (duckdb_v2_virtual_file_open_info_get_value(open_info, Convert("mem_hint"), &value, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_file_stat_handle listed = nullptr;
	duckdb_v2_virtual_file_open_info_get_stat(open_info, &listed, err);
	idx_t listed_size = 0;
	bool listed_size_known = false;
	duckdb_v2_file_stat_get_size(listed, &listed_size, &listed_size_known, err);
	duckdb_v2_context_handle context = nullptr;
	duckdb_v2_virtual_file_open_info_get_context(open_info, &context, err);

	// Attach the state first: a failure after this point must still destroy it.
	auto handle = new MemHandle();
	handle->fs = &fs;
	handle->path = path;
	duckdb_v2_opaque data {handle, MemDestroyHandle, nullptr};
	duckdb_v2_virtual_file_open_info_set_file_data(open_info, &data, err);
	duckdb_v2_virtual_file_open_info_set_property(open_info, DUCKDB_V2_FILE_PROPERTY_IS_SEEKABLE, fs.seekable, err);
	duckdb_v2_virtual_file_open_info_set_property(open_info, DUCKDB_V2_FILE_PROPERTY_IS_ON_DISK, fs.on_disk, err);

	std::lock_guard<std::mutex> guard(fs.lock);
	fs.last_open_path = path;
	fs.last_open_for_write = write || append;
	fs.last_open_parallel = parallel;
	fs.last_open_had_context = context != nullptr;
	fs.last_open_value = value ? Render(value) : "";
	if (listed_size_known) {
		fs.listed_sizes[path] = listed_size;
	}
	duckdb_v2_value_destroy(&value);

	auto exists = fs.files.count(path) > 0;
	if (exists && exclusive) {
		fs.failed_opens++;
		VfsFail(err, DUCKDB_V2_ERROR_IO_GENERAL, "mem: file already exists: " + path);
		return;
	}
	if (!exists) {
		if (create || create_new) {
			fs.files[path] = std::string();
		} else {
			fs.failed_opens++;
			VfsFail(err, DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND, "mem: no such file: " + path);
			return;
		}
	} else if (create_new) {
		fs.files[path].clear();
	}
	if (append) {
		handle->cursor = fs.files[path].size();
	}
	fs.opens++;
}

void MemClose(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle,
              duckdb_v2_error_info_handle *err) {
	VfsOf(info, err).closes++;
}

void MemAbort(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle file,
              duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto &handle = VfsFile(file, err);
	fs.aborts++;
	// Abandon what was written: the file never becomes visible.
	std::lock_guard<std::mutex> guard(fs.lock);
	fs.files.erase(handle.path);
}

void MemReadAt(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle file, void *buffer,
               idx_t buffer_size, idx_t location, idx_t *bytes_read, duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto &handle = VfsFile(file, err);
	fs.read_at_calls++;
	std::lock_guard<std::mutex> guard(fs.lock);
	auto &data = fs.files[handle.path];
	if (location >= data.size()) {
		*bytes_read = 0;
		return;
	}
	// pread semantics: a short read at the end, never an error.
	auto count = std::min<idx_t>(buffer_size, data.size() - location);
	std::memcpy(buffer, data.data() + location, count);
	*bytes_read = count;
}

void MemWriteAt(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle file,
                const void *buffer, idx_t buffer_size, idx_t location, duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto &handle = VfsFile(file, err);
	fs.write_at_calls++;
	std::lock_guard<std::mutex> guard(fs.lock);
	auto &data = fs.files[handle.path];
	if (location + buffer_size > data.size()) {
		data.resize(location + buffer_size, '\0');
	}
	std::memcpy(&data[location], buffer, buffer_size);
}

void MemRead(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle file, void *buffer,
             idx_t buffer_size, idx_t *bytes_read, duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto &handle = VfsFile(file, err);
	fs.read_calls++;
	std::lock_guard<std::mutex> guard(fs.lock);
	auto &data = fs.files[handle.path];
	if (handle.cursor >= data.size()) {
		*bytes_read = 0;
		return;
	}
	auto count = std::min<idx_t>(buffer_size, data.size() - handle.cursor);
	std::memcpy(buffer, data.data() + handle.cursor, count);
	handle.cursor += count;
	*bytes_read = count;
}

void MemWrite(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle file,
              const void *buffer, idx_t buffer_size, duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto &handle = VfsFile(file, err);
	fs.write_calls++;
	std::lock_guard<std::mutex> guard(fs.lock);
	auto &data = fs.files[handle.path];
	if (handle.cursor + buffer_size > data.size()) {
		data.resize(handle.cursor + buffer_size, '\0');
	}
	std::memcpy(&data[handle.cursor], buffer, buffer_size);
	handle.cursor += buffer_size;
}

void MemSeek(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle file, idx_t position,
             duckdb_v2_error_info_handle *err) {
	VfsOf(info, err).seek_calls++;
	VfsFile(file, err).cursor = position;
}

void MemTell(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle file, idx_t *position,
             duckdb_v2_error_info_handle *err) {
	VfsOf(info, err).tell_calls++;
	*position = VfsFile(file, err).cursor;
}

void MemStat(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle file,
             duckdb_v2_file_stat_handle stat, duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto &handle = VfsFile(file, err);
	std::lock_guard<std::mutex> guard(fs.lock);
	duckdb_v2_file_stat_set_size(stat, fs.files[handle.path].size(), err);
}

void MemSync(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle,
             duckdb_v2_error_info_handle *err) {
	VfsOf(info, err).sync_calls++;
}

void MemTruncate(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle file, idx_t size,
                 duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto &handle = VfsFile(file, err);
	std::lock_guard<std::mutex> guard(fs.lock);
	fs.files[handle.path].resize(size);
}

void MemStatPath(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_path_info_handle path_info,
                 duckdb_v2_file_stat_handle stat, duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto path = VfsPath(path_info, err);
	// Only a move has a target: latch what asking anyway reports.
	duckdb_v2_str target {nullptr, 0};
	fs.target_path_rc = duckdb_v2_virtual_file_path_info_get_target_path(path_info, &target, nullptr);
	std::lock_guard<std::mutex> guard(fs.lock);
	auto file = fs.files.find(path);
	if (file != fs.files.end()) {
		duckdb_v2_file_stat_set_type(stat, DUCKDB_V2_FILE_TYPE_REGULAR, err);
		duckdb_v2_file_stat_set_size(stat, file->second.size(), err);
		return;
	}
	if (fs.IsDirectory(path)) {
		duckdb_v2_file_stat_set_type(stat, DUCKDB_V2_FILE_TYPE_DIRECTORY, err);
	}
}

void MemList(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_path_info_handle path_info,
             duckdb_v2_file_listing_handle list, duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto prefix = VfsPath(path_info, err);
	if (prefix.back() != '/') {
		prefix += '/';
	}
	std::lock_guard<std::mutex> guard(fs.lock);
	std::set<std::string> seen;
	auto add = [&](const std::string &full, bool is_dir) {
		if (full.compare(0, prefix.size(), prefix) != 0) {
			return;
		}
		auto rest = full.substr(prefix.size());
		auto slash = rest.find('/');
		auto name = slash == std::string::npos ? rest : rest.substr(0, slash);
		if (name.empty() || !seen.insert(name).second) {
			return;
		}
		auto type =
		    (slash != std::string::npos || is_dir) ? DUCKDB_V2_FILE_TYPE_DIRECTORY : DUCKDB_V2_FILE_TYPE_REGULAR;
		duckdb_v2_file_stat_handle stat = nullptr;
		duckdb_v2_file_listing_add_entry(list, Convert(name), type, &stat, err);
		if (type == DUCKDB_V2_FILE_TYPE_REGULAR && stat) {
			// The listing knows the size; hand it along so the open sees it.
			duckdb_v2_file_stat_set_size(stat, fs.files[full].size(), err);
		}
	};
	for (auto &entry : fs.files) {
		add(entry.first, false);
	}
	for (auto &dir : fs.dirs) {
		add(dir, true);
	}
}

void MemRemoveFile(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_path_info_handle path_info,
                   duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto path = VfsPath(path_info, err);
	std::lock_guard<std::mutex> guard(fs.lock);
	if (fs.files.erase(path) == 0) {
		VfsFail(err, DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND, "mem: no such file: " + path);
	}
}

void MemCreateDirectory(duckdb_v2_virtual_file_system_info_handle info,
                        duckdb_v2_virtual_file_path_info_handle path_info, duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto path = VfsPath(path_info, err);
	std::lock_guard<std::mutex> guard(fs.lock);
	fs.dirs.insert(path);
}

void MemRemoveDirectory(duckdb_v2_virtual_file_system_info_handle info,
                        duckdb_v2_virtual_file_path_info_handle path_info, duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto path = VfsPath(path_info, err);
	auto prefix = path + "/";
	std::lock_guard<std::mutex> guard(fs.lock);
	for (auto it = fs.files.begin(); it != fs.files.end();) {
		if (it->first.compare(0, prefix.size(), prefix) == 0) {
			it = fs.files.erase(it);
		} else {
			++it;
		}
	}
	for (auto it = fs.dirs.begin(); it != fs.dirs.end();) {
		if (*it == path || it->compare(0, prefix.size(), prefix) == 0) {
			it = fs.dirs.erase(it);
		} else {
			++it;
		}
	}
}

void MemMove(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_path_info_handle path_info,
             duckdb_v2_error_info_handle *err) {
	auto &fs = VfsOf(info, err);
	auto source = VfsPath(path_info, err);
	duckdb_v2_str target_view {nullptr, 0};
	if (duckdb_v2_virtual_file_path_info_get_target_path(path_info, &target_view, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto target = Convert(target_view);
	std::lock_guard<std::mutex> guard(fs.lock);
	auto it = fs.files.find(source);
	if (it == fs.files.end()) {
		VfsFail(err, DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND, "mem: no such file: " + source);
		return;
	}
	fs.files[target] = std::move(it->second);
	fs.files.erase(it);
}

void MemDestroyUserData(void *data) {
	static_cast<MemFs *>(data)->user_data_destroyed++;
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

struct MemFsOptions {
	bool cursor_callbacks = false; // read / write / seek / tell
	bool write_at = true;
	bool path_callbacks = true; // stat path / list / remove / mkdir / rmdir / move
	bool close_callback = true;
	bool abort_callback = false;
	bool route_by_callback = false; // a claim callback instead of a prefix
};

// Builds and registers the in-memory file system with the given callbacks.
void RegisterMemFs(duckdb_v2_connection_handle conn, MemFs &fs, const MemFsOptions &options = MemFsOptions(),
                   const char *name = "mem") {
	duckdb_v2_virtual_file_system_handle vfs = nullptr;
	REQUIRE(duckdb_v2_virtual_file_system_create_with_connection(conn, &vfs, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_name(vfs, Convert(name), nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_opaque user_data {&fs, MemDestroyUserData, nullptr};
	REQUIRE(duckdb_v2_virtual_file_system_set_user_data(vfs, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);
	if (options.route_by_callback) {
		REQUIRE(duckdb_v2_virtual_file_system_set_claim_callback(vfs, MemClaim, nullptr) == DUCKDB_V2_ERROR_NONE);
	} else {
		REQUIRE(duckdb_v2_virtual_file_system_add_prefix(vfs, Convert(SCHEME), nullptr) == DUCKDB_V2_ERROR_NONE);
	}

	REQUIRE(duckdb_v2_virtual_file_system_set_open_callback(vfs, MemOpen, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_read_at_callback(vfs, MemReadAt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_stat_callback(vfs, MemStat, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_sync_callback(vfs, MemSync, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_truncate_callback(vfs, MemTruncate, nullptr) == DUCKDB_V2_ERROR_NONE);
	if (options.close_callback) {
		REQUIRE(duckdb_v2_virtual_file_system_set_close_callback(vfs, MemClose, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	if (options.abort_callback) {
		REQUIRE(duckdb_v2_virtual_file_system_set_abort_callback(vfs, MemAbort, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	if (options.write_at) {
		REQUIRE(duckdb_v2_virtual_file_system_set_write_at_callback(vfs, MemWriteAt, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	if (options.cursor_callbacks) {
		REQUIRE(duckdb_v2_virtual_file_system_set_read_callback(vfs, MemRead, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_virtual_file_system_set_write_callback(vfs, MemWrite, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_virtual_file_system_set_seek_callback(vfs, MemSeek, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_virtual_file_system_set_tell_callback(vfs, MemTell, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	if (options.path_callbacks) {
		REQUIRE(duckdb_v2_virtual_file_system_set_stat_path_callback(vfs, MemStatPath, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_virtual_file_system_set_list_callback(vfs, MemList, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_virtual_file_system_set_remove_file_callback(vfs, MemRemoveFile, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_virtual_file_system_set_create_directory_callback(vfs, MemCreateDirectory, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_virtual_file_system_set_remove_directory_callback(vfs, MemRemoveDirectory, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_virtual_file_system_set_move_callback(vfs, MemMove, nullptr) == DUCKDB_V2_ERROR_NONE);
	}

	REQUIRE(duckdb_v2_virtual_file_system_register(vfs, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_destroy(&vfs) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(vfs == nullptr);
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

std::string ErrorTextOf(duckdb_v2_error_info_handle *err) {
	REQUIRE(*err != nullptr);
	duckdb_v2_str text {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(*err, &text) == DUCKDB_V2_ERROR_NONE);
	auto out = Convert(text);
	duckdb_v2_error_info_destroy(err);
	return out;
}

// Runs a query and collects the first column as strings, casting in SQL so any type renders.
std::vector<std::string> VfsQueryStrings(duckdb_v2_connection_handle conn, const std::string &sql) {
	auto wrapped = "SELECT CAST(#1 AS VARCHAR) FROM (" + sql + ")";
	duckdb_v2_result_handle result = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = Query(conn, wrapped.c_str(), &result, &err);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		// Catch's INFO does not surface under the suite's reporter; say what went wrong directly.
		fprintf(stderr, "query failed: %s\n", ErrorTextOf(&err).c_str());
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	std::vector<std::string> out;
	while (auto chunk = StepChunk(result)) {
		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		duckdb_v2_vector_handle vec = nullptr;
		duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
		duckdb_v2_vector_view view {};
		duckdb_v2_vector_get_view(vec, &view, nullptr);
		auto data = static_cast<const duckdb_v2_bytes *>(view.data);
		for (idx_t i = 0; i < size; i++) {
			out.push_back(Convert(Convert(data[SelAt(view.sel, i)])));
		}
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	duckdb_v2_result_destroy(&result);
	return out;
}

// Runs a query expected to fail, returning the error text.
std::string VfsQueryError(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle result = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = Query(conn, sql, &result, &err);
	if (rc == DUCKDB_V2_ERROR_NONE) {
		// Execution is lazy: the failure may only surface once the result is stepped.
		auto status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		while (rc == DUCKDB_V2_ERROR_NONE && status != DUCKDB_V2_RESULT_STEP_STATUS_FINISHED) {
			duckdb_v2_data_chunk_handle chunk = nullptr;
			rc = duckdb_v2_result_step(result, &chunk, &status, &err);
			if (chunk) {
				duckdb_v2_data_chunk_destroy(&chunk);
			}
			if (rc == DUCKDB_V2_ERROR_NONE && status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
				rc = duckdb_v2_result_wait(result, &err);
			}
		}
		duckdb_v2_result_destroy(&result);
	}
	REQUIRE(rc != DUCKDB_V2_ERROR_NONE);
	return ErrorTextOf(&err);
}

// C file API helpers
duckdb_v2_file_system_handle VfsEngineFs(duckdb_v2_connection_handle conn) {
	duckdb_v2_file_system_handle fs = nullptr;
	REQUIRE(duckdb_v2_file_system_get_from_connection(conn, &fs, nullptr) == DUCKDB_V2_ERROR_NONE);
	return fs;
}

DUCKDB_V2_ERROR VfsTryOpen(duckdb_v2_file_system_handle fs, const std::string &path,
                           const std::vector<DUCKDB_V2_FILE_FLAG> &flags, duckdb_v2_file_handle *out,
                           duckdb_v2_error_info_handle *err = nullptr, duckdb_v2_value_handle hint = nullptr) {
	duckdb_v2_file_open_options_handle options = nullptr;
	REQUIRE(duckdb_v2_file_open_options_create(fs, &options, nullptr) == DUCKDB_V2_ERROR_NONE);
	for (auto flag : flags) {
		REQUIRE(duckdb_v2_file_open_options_set_flag(options, flag, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	if (hint) {
		REQUIRE(duckdb_v2_file_open_options_set_value(options, Convert("mem_hint"), hint, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
	}
	auto rc = duckdb_v2_file_system_open(fs, Convert(path), options, out, err);
	duckdb_v2_file_open_options_destroy(&options);
	return rc;
}

duckdb_v2_file_handle VfsOpen(duckdb_v2_file_system_handle fs, const std::string &path,
                              const std::vector<DUCKDB_V2_FILE_FLAG> &flags) {
	duckdb_v2_file_handle handle = nullptr;
	REQUIRE(VfsTryOpen(fs, path, flags, &handle) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(handle != nullptr);
	return handle;
}

const char *CSV_A = "i,s\n1,one\n2,two\n";
const char *CSV_B = "i,s\n3,three\n";
const char *CSV_C = "i,s\n4,four\n5,five\n6,six\n";

} // namespace

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

TEST_CASE("V2 virtual file system: registration requires a name, routing and the core callbacks", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;

	duckdb_v2_virtual_file_system_handle vfs = nullptr;
	REQUIRE(duckdb_v2_virtual_file_system_create_with_connection(fx.conn, &vfs, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto expect_failure = [&](const char *needle) {
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_virtual_file_system_register(vfs, &err) != DUCKDB_V2_ERROR_NONE);
		REQUIRE(ErrorTextOf(&err).find(needle) != std::string::npos);
	};

	expect_failure("name");
	REQUIRE(duckdb_v2_virtual_file_system_set_name(vfs, Convert("mem"), nullptr) == DUCKDB_V2_ERROR_NONE);
	expect_failure("prefix or a claim callback");
	REQUIRE(duckdb_v2_virtual_file_system_add_prefix(vfs, Convert(""), nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_virtual_file_system_add_prefix(vfs, Convert(SCHEME), nullptr) == DUCKDB_V2_ERROR_NONE);
	expect_failure("Open");
	REQUIRE(duckdb_v2_virtual_file_system_set_open_callback(vfs, MemOpen, nullptr) == DUCKDB_V2_ERROR_NONE);
	expect_failure("Read at");
	REQUIRE(duckdb_v2_virtual_file_system_set_read_at_callback(vfs, MemReadAt, nullptr) == DUCKDB_V2_ERROR_NONE);
	expect_failure("Stat");
	REQUIRE(duckdb_v2_virtual_file_system_set_stat_callback(vfs, MemStat, nullptr) == DUCKDB_V2_ERROR_NONE);
	// Owning the cursor needs tell, and read since the file system has read at.
	REQUIRE(duckdb_v2_virtual_file_system_set_seek_callback(vfs, MemSeek, nullptr) == DUCKDB_V2_ERROR_NONE);
	expect_failure("Tell");
	REQUIRE(duckdb_v2_virtual_file_system_set_tell_callback(vfs, MemTell, nullptr) == DUCKDB_V2_ERROR_NONE);
	expect_failure("Read callback");
	REQUIRE(duckdb_v2_virtual_file_system_set_read_callback(vfs, MemRead, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_opaque user_data {&mem, nullptr, nullptr};
	REQUIRE(duckdb_v2_virtual_file_system_set_user_data(vfs, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_register(vfs, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The name is now taken.
	expect_failure("already been registered");

	REQUIRE(duckdb_v2_virtual_file_system_destroy(&vfs) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(vfs == nullptr);
	// Null-safe.
	REQUIRE(duckdb_v2_virtual_file_system_destroy(&vfs) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);

	// Null arguments are reported, not dereferenced.
	REQUIRE(duckdb_v2_virtual_file_system_create_with_connection(nullptr, &vfs, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_virtual_file_system_register(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_virtual_file_open_info_set_property(nullptr, DUCKDB_V2_FILE_PROPERTY_IS_ON_DISK, true, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
}

TEST_CASE("V2 virtual file system: a claim callback routes paths without a prefix", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://data/a.csv"] = CSV_A;
	MemFsOptions options;
	options.route_by_callback = true;
	RegisterMemFs(fx.conn, mem, options);

	auto rows = VfsQueryStrings(fx.conn, "SELECT count(*) FROM read_csv('mem://data/a.csv')");
	REQUIRE(rows == std::vector<std::string> {"2"});
	auto other = VfsQueryError(fx.conn, "SELECT * FROM read_csv('other://data/a.csv')");
	REQUIRE(other.find("mem:") == std::string::npos);
}

// ---------------------------------------------------------------------------
// Reading through SQL
// ---------------------------------------------------------------------------

TEST_CASE("V2 virtual file system: read_csv reads a seekable file at offsets", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://data/a.csv"] = CSV_A;
	RegisterMemFs(fx.conn, mem);

	auto rows = VfsQueryStrings(fx.conn, "SELECT s FROM read_csv('mem://data/a.csv') ORDER BY i");
	REQUIRE(rows == std::vector<std::string> {"one", "two"});

	REQUIRE(mem.read_at_calls > 0);
	REQUIRE(mem.read_calls == 0);
	REQUIRE(mem.opens > 0);
	REQUIRE(mem.opens == mem.closes);
	REQUIRE(mem.opens == mem.file_data_destroyed);
	// Opened from inside a query, so the open saw a context.
	REQUIRE(mem.last_open_had_context);
}

TEST_CASE("V2 virtual file system: a stream-like file is read through its cursor", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://data/a.csv"] = CSV_A;
	MemFsOptions options;
	options.cursor_callbacks = true;
	// Seekable files are read at offsets, cache or no cache; only a file that cannot seek is streamed.
	mem.seekable = false;
	RegisterMemFs(fx.conn, mem, options);

	auto rows = VfsQueryStrings(fx.conn, "SELECT i FROM read_csv('mem://data/a.csv') ORDER BY i");
	REQUIRE(rows == std::vector<std::string> {"1", "2"});
	REQUIRE(mem.read_calls > 0);
	REQUIRE(mem.read_at_calls == 0);
}

TEST_CASE("V2 virtual file system: globs are expanded over the list callback", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://data/a.csv"] = CSV_A;
	mem.files["mem://data/b.csv"] = CSV_B;
	mem.files["mem://data/notes.txt"] = "not a csv";
	mem.files["mem://data/sub/c.csv"] = CSV_C;
	RegisterMemFs(fx.conn, mem);

	auto direct = VfsQueryStrings(fx.conn, "SELECT file FROM glob('mem://data/*.csv')");
	REQUIRE(direct == std::vector<std::string> {"mem://data/a.csv", "mem://data/b.csv"});

	auto question = VfsQueryStrings(fx.conn, "SELECT file FROM glob('mem://data/?.csv')");
	REQUIRE(question == std::vector<std::string> {"mem://data/a.csv", "mem://data/b.csv"});

	auto nested = VfsQueryStrings(fx.conn, "SELECT file FROM glob('mem://data/*/*.csv')");
	REQUIRE(nested == std::vector<std::string> {"mem://data/sub/c.csv"});

	auto crawl = VfsQueryStrings(fx.conn, "SELECT file FROM glob('mem://data/**/*.csv')");
	REQUIRE(crawl == std::vector<std::string> {"mem://data/a.csv", "mem://data/b.csv", "mem://data/sub/c.csv"});

	auto everything = VfsQueryStrings(fx.conn, "SELECT file FROM glob('mem://**')");
	REQUIRE(everything.size() == 4);

	auto nothing = VfsQueryStrings(fx.conn, "SELECT file FROM glob('mem://data/*.parquet')");
	REQUIRE(nothing.empty());

	// A plain path that exists is its own expansion; one that does not is no expansion.
	auto plain = VfsQueryStrings(fx.conn, "SELECT file FROM glob('mem://data/sub/c.csv')");
	REQUIRE(plain == std::vector<std::string> {"mem://data/sub/c.csv"});
	auto missing = VfsQueryStrings(fx.conn, "SELECT file FROM glob('mem://data/missing.csv')");
	REQUIRE(missing.empty());

	// The whole thing feeds a multi-file reader, and what the listing knew reaches the open.
	auto rows = VfsQueryStrings(fx.conn, "SELECT count(*) FROM read_csv('mem://data/**/*.csv')");
	REQUIRE(rows == std::vector<std::string> {"6"});
	// The first file of a scan is reopened from its bare path for sniffing, so only later files carry it.
	REQUIRE(mem.listed_sizes["mem://data/sub/c.csv"] == std::strlen(CSV_C));

	// Only a move has a target path.
	REQUIRE(mem.target_path_rc == DUCKDB_V2_ERROR_INPUT_INVALID);
}

TEST_CASE("V2 virtual file system: a glob callback replaces the generic expansion", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://data/a.csv"] = CSV_A;
	mem.files["mem://data/b.csv"] = CSV_B;

	duckdb_v2_virtual_file_system_handle vfs = nullptr;
	REQUIRE(duckdb_v2_virtual_file_system_create_with_connection(fx.conn, &vfs, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_name(vfs, Convert("mem"), nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_opaque user_data {&mem, nullptr, nullptr};
	REQUIRE(duckdb_v2_virtual_file_system_set_user_data(vfs, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_add_prefix(vfs, Convert(SCHEME), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_open_callback(vfs, MemOpen, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_read_at_callback(vfs, MemReadAt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_stat_callback(vfs, MemStat, nullptr) == DUCKDB_V2_ERROR_NONE);
	// A glob that ignores the pattern and always reports b.csv: proves the callback, not the listing, was used.
	REQUIRE(duckdb_v2_virtual_file_system_set_glob_callback(
	            vfs,
	            [](duckdb_v2_virtual_file_system_info_handle, duckdb_v2_virtual_file_path_info_handle,
	               duckdb_v2_file_listing_handle list, duckdb_v2_error_info_handle *err) {
		            // The stat out-parameter is optional.
		            duckdb_v2_file_listing_add_entry(list, Convert("mem://data/b.csv"), DUCKDB_V2_FILE_TYPE_REGULAR,
		                                             nullptr, err);
	            },
	            nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_register(vfs, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_virtual_file_system_destroy(&vfs);

	auto files = VfsQueryStrings(fx.conn, "SELECT file FROM glob('mem://data/*.csv')");
	REQUIRE(files == std::vector<std::string> {"mem://data/b.csv"});
	auto rows = VfsQueryStrings(fx.conn, "SELECT s FROM read_csv('mem://data/a*.csv')");
	REQUIRE(rows == std::vector<std::string> {"three"});
}

TEST_CASE("V2 virtual file system: a pattern with neither list nor glob callback is an error", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://data/a.csv"] = CSV_A;
	MemFsOptions options;
	options.path_callbacks = false;
	RegisterMemFs(fx.conn, mem, options);

	// A plain path still works: it needs no listing.
	auto rows = VfsQueryStrings(fx.conn, "SELECT count(*) FROM read_csv('mem://data/a.csv')");
	REQUIRE(rows == std::vector<std::string> {"2"});

	auto text = VfsQueryError(fx.conn, "SELECT * FROM read_csv('mem://data/*.csv')");
	REQUIRE(text.find("no list or glob callback") != std::string::npos);
}

// ---------------------------------------------------------------------------
// Writing through SQL
// ---------------------------------------------------------------------------

TEST_CASE("V2 virtual file system: COPY TO writes through the cursor callbacks", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	MemFsOptions options;
	options.cursor_callbacks = true;
	RegisterMemFs(fx.conn, mem, options);

	ExecSQL(fx.conn, "COPY (SELECT range AS i, 'v' || range AS s FROM range(5)) TO 'mem://out/x.csv' (HEADER)");
	REQUIRE(mem.write_calls > 0);
	REQUIRE(mem.files.count("mem://out/x.csv") == 1);

	auto rows = VfsQueryStrings(fx.conn, "SELECT s FROM read_csv('mem://out/x.csv') ORDER BY i");
	REQUIRE(rows == std::vector<std::string> {"v0", "v1", "v2", "v3", "v4"});

	// Overwriting truncates through the open flags.
	ExecSQL(fx.conn, "COPY (SELECT 42 AS i, 'again' AS s) TO 'mem://out/x.csv' (HEADER)");
	auto again = VfsQueryStrings(fx.conn, "SELECT s FROM read_csv('mem://out/x.csv')");
	REQUIRE(again == std::vector<std::string> {"again"});

	REQUIRE(mem.opens == mem.closes);
	REQUIRE(mem.opens == mem.file_data_destroyed);
}

TEST_CASE("V2 virtual file system: COPY TO is served through write_at when the engine keeps the cursor",
          "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	RegisterMemFs(fx.conn, mem);

	ExecSQL(fx.conn, "COPY (SELECT range AS i FROM range(3)) TO 'mem://out/y.csv' (HEADER)");
	REQUIRE(mem.write_at_calls > 0);
	REQUIRE(mem.write_calls == 0);
	// A written file is synced before it is closed.
	REQUIRE(mem.sync_calls > 0);

	auto rows = VfsQueryStrings(fx.conn, "SELECT i FROM read_csv('mem://out/y.csv') ORDER BY i");
	REQUIRE(rows == std::vector<std::string> {"0", "1", "2"});
}

TEST_CASE("V2 virtual file system: partitioned COPY TO uses the directory callbacks", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	MemFsOptions options;
	options.cursor_callbacks = true;
	RegisterMemFs(fx.conn, mem, options);

	ExecSQL(fx.conn, "COPY (SELECT range % 2 AS p, range AS i FROM range(6)) TO 'mem://part' "
	                 "(FORMAT CSV, PARTITION_BY (p))");
	REQUIRE(mem.dirs.count("mem://part/p=0") == 1);
	REQUIRE(mem.dirs.count("mem://part/p=1") == 1);

	auto rows =
	    VfsQueryStrings(fx.conn, "SELECT count(*) FROM read_csv('mem://part/**/*.csv', hive_partitioning = true) "
	                             "WHERE p = 1");
	REQUIRE(rows == std::vector<std::string> {"3"});
}

TEST_CASE("V2 virtual file system: a file system without write callbacks is read-only", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://data/a.csv"] = CSV_A;
	MemFsOptions options;
	options.write_at = false;
	RegisterMemFs(fx.conn, mem, options);

	auto rows = VfsQueryStrings(fx.conn, "SELECT count(*) FROM read_csv('mem://data/a.csv')");
	REQUIRE(rows == std::vector<std::string> {"2"});

	auto text = VfsQueryError(fx.conn, "COPY (SELECT 1) TO 'mem://out/z.csv'");
	REQUIRE(text.find("read-only") != std::string::npos);
	REQUIRE(mem.files.count("mem://out/z.csv") == 0);
}

TEST_CASE("V2 virtual file system: a failed COPY aborts the file instead of closing it", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	MemFsOptions options;
	options.cursor_callbacks = true;
	options.abort_callback = true;
	RegisterMemFs(fx.conn, mem, options);

	auto text = VfsQueryError(fx.conn, "COPY (SELECT CASE WHEN range = 3 THEN error('boom') ELSE range END AS i "
	                                   "FROM range(5)) TO 'mem://out/partial.csv'");
	REQUIRE(text.find("boom") != std::string::npos);
	REQUIRE(mem.aborts == 1);
	// Abort takes the place of close, and the file data is still released.
	REQUIRE(mem.closes == 0);
	REQUIRE(mem.opens == mem.file_data_destroyed);
	REQUIRE(mem.files.count("mem://out/partial.csv") == 0);
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

TEST_CASE("V2 virtual file system: callback errors surface with their text", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	// Without a stat path callback a plain path is handed straight to open, whose error is the one reported.
	MemFsOptions options;
	options.path_callbacks = false;
	RegisterMemFs(fx.conn, mem, options);

	auto text = VfsQueryError(fx.conn, "SELECT * FROM read_csv('mem://data/missing.csv')");
	REQUIRE(text.find("mem: no such file: mem://data/missing.csv") != std::string::npos);
	// The open attached its state before failing; the failure released it.
	REQUIRE(mem.failed_opens == 1);
	REQUIRE(mem.file_data_destroyed == mem.opens + mem.failed_opens);

	// Paths the file system does not claim go elsewhere.
	auto other = VfsQueryError(fx.conn, "SELECT * FROM read_csv('other://data/missing.csv')");
	REQUIRE(other.find("mem:") == std::string::npos);
}

// ---------------------------------------------------------------------------
// The C file API against the registered file system
// ---------------------------------------------------------------------------

TEST_CASE("V2 virtual file system: parallel access reads and writes at offsets", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	MemFsOptions options;
	options.cursor_callbacks = true;
	RegisterMemFs(fx.conn, mem, options);
	auto fs = VfsEngineFs(fx.conn);

	{
		auto handle =
		    VfsOpen(fs, "mem://p.bin",
		            {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW, DUCKDB_V2_FILE_FLAG_PARALLEL_ACCESS});
		REQUIRE(mem.last_open_parallel);
		REQUIRE(mem.last_open_for_write);
		REQUIRE(duckdb_v2_file_write_at(handle, "world", 5, 6, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_file_write_at(handle, "hello ", 6, 0, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_file_sync(handle, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_file_destroy(&handle);
	}
	REQUIRE(mem.files["mem://p.bin"] == "hello world");
	REQUIRE(mem.write_at_calls == 2);
	REQUIRE(mem.write_calls == 0);
	REQUIRE(mem.sync_calls >= 1);

	{
		auto handle = VfsOpen(fs, "mem://p.bin", {DUCKDB_V2_FILE_FLAG_READ, DUCKDB_V2_FILE_FLAG_PARALLEL_ACCESS});
		char buffer[5];
		REQUIRE(duckdb_v2_file_read_at(handle, buffer, 5, 6, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(std::string(buffer, 5) == "world");
		// The callback comes up short past the end; the engine's offset read is all or nothing.
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_file_read_at(handle, buffer, 5, 9, &err) != DUCKDB_V2_ERROR_NONE);
		REQUIRE(ErrorTextOf(&err).find("Could not read all bytes") != std::string::npos);
		duckdb_v2_file_destroy(&handle);
	}
	REQUIRE(mem.read_calls == 0);
	REQUIRE(mem.opens == mem.closes);
}

TEST_CASE("V2 virtual file system: parallel writes need a write_at callback", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	MemFsOptions options;
	options.cursor_callbacks = true;
	options.write_at = false;
	RegisterMemFs(fx.conn, mem, options);
	auto fs = VfsEngineFs(fx.conn);

	duckdb_v2_file_handle handle = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(VfsTryOpen(fs, "mem://q.bin",
	                   {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE, DUCKDB_V2_FILE_FLAG_PARALLEL_ACCESS},
	                   &handle, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(handle == nullptr);
	REQUIRE(ErrorTextOf(&err).find("write at") != std::string::npos);
	// The open callback was never reached.
	REQUIRE(mem.opens == 0);

	// Without parallel access the cursor write serves it.
	handle = VfsOpen(fs, "mem://q.bin", {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE});
	idx_t written = 0;
	REQUIRE(duckdb_v2_file_write(handle, "abc", 3, &written, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(written == 3);
	duckdb_v2_file_destroy(&handle);
	REQUIRE(mem.write_calls == 1);
	REQUIRE(mem.files["mem://q.bin"] == "abc");
}

TEST_CASE("V2 virtual file system: the engine keeps the cursor when the file system has none", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://c.txt"] = "0123456789";
	RegisterMemFs(fx.conn, mem);
	auto fs = VfsEngineFs(fx.conn);

	auto handle = VfsOpen(fs, "mem://c.txt", {DUCKDB_V2_FILE_FLAG_READ});
	char buffer[4];
	idx_t read = 0;
	REQUIRE(duckdb_v2_file_read(handle, buffer, 4, &read, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(buffer, read) == "0123");
	idx_t position = 0;
	REQUIRE(duckdb_v2_file_tell(handle, &position, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(position == 4);

	REQUIRE(duckdb_v2_file_seek(handle, 8, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_read(handle, buffer, 4, &read, nullptr) == DUCKDB_V2_ERROR_NONE);
	// A short read at the end, straight from read_at.
	REQUIRE(std::string(buffer, read) == "89");
	REQUIRE(duckdb_v2_file_read(handle, buffer, 4, &read, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(read == 0);
	REQUIRE(mem.read_calls == 0);
	REQUIRE(mem.seek_calls == 0);
	REQUIRE(mem.tell_calls == 0);
	duckdb_v2_file_destroy(&handle);

	// Appending through write_at starts the engine's cursor at the end.
	handle = VfsOpen(fs, "mem://c.txt", {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_APPEND});
	idx_t written = 0;
	REQUIRE(duckdb_v2_file_write(handle, "ab", 2, &written, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_file_destroy(&handle);
	REQUIRE(mem.files["mem://c.txt"] == "0123456789ab");
	REQUIRE(mem.write_at_calls == 1);
}

TEST_CASE("V2 virtual file system: the file system owns the cursor", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://c.txt"] = "0123456789";
	MemFsOptions options;
	options.cursor_callbacks = true;
	RegisterMemFs(fx.conn, mem, options);
	auto fs = VfsEngineFs(fx.conn);

	auto handle = VfsOpen(fs, "mem://c.txt", {DUCKDB_V2_FILE_FLAG_READ});
	char buffer[4];
	idx_t read = 0;
	REQUIRE(duckdb_v2_file_read(handle, buffer, 4, &read, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(buffer, read) == "0123");
	REQUIRE(mem.read_calls == 1);

	idx_t position = 0;
	REQUIRE(duckdb_v2_file_tell(handle, &position, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(position == 4);
	REQUIRE(mem.tell_calls == 1);

	REQUIRE(duckdb_v2_file_seek(handle, 8, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(mem.seek_calls == 1);
	REQUIRE(duckdb_v2_file_read(handle, buffer, 4, &read, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(buffer, read) == "89");
	REQUIRE(duckdb_v2_file_read(handle, buffer, 4, &read, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(read == 0);

	// Offset reads never touch the cursor.
	REQUIRE(duckdb_v2_file_read_at(handle, buffer, 4, 0, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(buffer, 4) == "0123");
	REQUIRE(duckdb_v2_file_tell(handle, &position, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(position == 10);
	duckdb_v2_file_destroy(&handle);

	// Appending starts the cursor at the end, which is the open callback's job.
	handle = VfsOpen(fs, "mem://c.txt", {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_APPEND});
	idx_t written = 0;
	REQUIRE(duckdb_v2_file_write(handle, "ab", 2, &written, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_file_destroy(&handle);
	REQUIRE(mem.files["mem://c.txt"] == "0123456789ab");
}

TEST_CASE("V2 virtual file system: a cursor owner without a seek callback fails seeks loudly", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://c.txt"] = "0123456789";

	duckdb_v2_virtual_file_system_handle vfs = nullptr;
	REQUIRE(duckdb_v2_virtual_file_system_create_with_connection(fx.conn, &vfs, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_name(vfs, Convert("mem"), nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_opaque user_data {&mem, nullptr, nullptr};
	REQUIRE(duckdb_v2_virtual_file_system_set_user_data(vfs, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_add_prefix(vfs, Convert(SCHEME), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_open_callback(vfs, MemOpen, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_read_at_callback(vfs, MemReadAt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_stat_callback(vfs, MemStat, nullptr) == DUCKDB_V2_ERROR_NONE);
	// A stream-only cursor: read and tell, no seek.
	REQUIRE(duckdb_v2_virtual_file_system_set_read_callback(vfs, MemRead, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_tell_callback(vfs, MemTell, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_register(vfs, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_virtual_file_system_destroy(&vfs);
	auto fs = VfsEngineFs(fx.conn);

	auto handle = VfsOpen(fs, "mem://c.txt", {DUCKDB_V2_FILE_FLAG_READ});
	char buffer[4];
	idx_t read = 0;
	REQUIRE(duckdb_v2_file_read(handle, buffer, 4, &read, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(buffer, read) == "0123");
	REQUIRE(mem.read_calls == 1);
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_file_seek(handle, 0, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(ErrorTextOf(&err).find("no \"seek\" callback") != std::string::npos);
	duckdb_v2_file_destroy(&handle);
}

TEST_CASE("V2 virtual file system: the open callback sees flags and attached values", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://v.txt"] = "x";
	RegisterMemFs(fx.conn, mem);
	auto fs = VfsEngineFs(fx.conn);

	duckdb_v2_value_handle hint = nullptr;
	REQUIRE(duckdb_v2_value_create_int_with_connection(fx.conn, 42, &hint, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_file_handle handle = nullptr;
	REQUIRE(VfsTryOpen(fs, "mem://v.txt", {DUCKDB_V2_FILE_FLAG_READ}, &handle, nullptr, hint) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&hint);
	REQUIRE(mem.last_open_path == "mem://v.txt");
	REQUIRE(!mem.last_open_for_write);
	REQUIRE(!mem.last_open_parallel);
	REQUIRE(mem.last_open_value == "42");
	// A connection's file system opens with the connection's context in hand.
	REQUIRE(mem.last_open_had_context);
	duckdb_v2_file_destroy(&handle);

	// Without the value attached, the lookup comes back empty rather than failing.
	handle = VfsOpen(fs, "mem://v.txt", {DUCKDB_V2_FILE_FLAG_READ});
	REQUIRE(mem.last_open_value.empty());
	duckdb_v2_file_destroy(&handle);

	// Exclusive create is reported to the callback, which refuses an existing file.
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(VfsTryOpen(fs, "mem://v.txt",
	                   {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE, DUCKDB_V2_FILE_FLAG_EXCLUSIVE_CREATE},
	                   &handle, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(ErrorTextOf(&err).find("already exists") != std::string::npos);
}

TEST_CASE("V2 virtual file system: user data outlives the builder and dies with the database", "[capi_v2][vfs]") {
	MemFs mem;
	mem.files["mem://data/a.csv"] = CSV_A;
	{
		EnvFixture fx;
		RegisterMemFs(fx.conn, mem);
		// The builder is gone; the registered file system still reaches the user data.
		auto rows = VfsQueryStrings(fx.conn, "SELECT count(*) FROM read_csv('mem://data/a.csv')");
		REQUIRE(rows == std::vector<std::string> {"2"});
		REQUIRE(mem.user_data_destroyed == 0);
	}
	REQUIRE(mem.user_data_destroyed == 1);
	REQUIRE(mem.opens == mem.closes);
	REQUIRE(mem.opens == mem.file_data_destroyed);
}

TEST_CASE("V2 virtual file system: no close callback still destroys the file data", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://data/a.csv"] = CSV_A;
	MemFsOptions options;
	options.close_callback = false;
	RegisterMemFs(fx.conn, mem, options);

	auto rows = VfsQueryStrings(fx.conn, "SELECT count(*) FROM read_csv('mem://data/a.csv')");
	REQUIRE(rows == std::vector<std::string> {"2"});
	REQUIRE(mem.closes == 0);
	REQUIRE(mem.opens > 0);
	REQUIRE(mem.opens == mem.file_data_destroyed);
}

TEST_CASE("V2 virtual file system: a non-seekable file needs a file system that owns the cursor", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://s.txt"] = "stream";
	mem.seekable = false;
	RegisterMemFs(fx.conn, mem);
	auto fs = VfsEngineFs(fx.conn);

	duckdb_v2_file_handle handle = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(VfsTryOpen(fs, "mem://s.txt", {DUCKDB_V2_FILE_FLAG_READ}, &handle, &err) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(handle == nullptr);
	REQUIRE(ErrorTextOf(&err).find("owns the cursor") != std::string::npos);
	// The open callback ran and its state was released.
	REQUIRE(mem.opens == 1);
	REQUIRE(mem.file_data_destroyed == 1);
}

// ---------------------------------------------------------------------------
// Delegation: an overlay under its own scheme that forwards to what claims the path underneath
// ---------------------------------------------------------------------------

namespace {

// State of the overlay: counts what it forwards.
struct Overlay {
	std::atomic<int> opens {0};
	std::atomic<int> reads {0};
	std::atomic<int> lists {0};
	std::atomic<int> stats {0};
};

constexpr const char *OVERLAY_SCHEME = "cached+";

std::string Underneath(duckdb_v2_str path) {
	return Convert(path).substr(std::strlen(OVERLAY_SCHEME));
}

Overlay &OverlayOf(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_error_info_handle *err) {
	void *data = nullptr;
	duckdb_v2_virtual_file_system_info_get_user_data(info, &data, err);
	return *static_cast<Overlay *>(data);
}

// The file system to delegate to: the context's when the request has one, the database's otherwise.
duckdb_v2_file_system_handle DelegateFs(duckdb_v2_virtual_file_system_info_handle info,
                                        duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	duckdb_v2_file_system_handle fs = nullptr;
	if (context) {
		duckdb_v2_file_system_get_from_context(context, &fs, err);
	} else {
		duckdb_v2_virtual_file_system_info_get_file_system(info, &fs, err);
	}
	return fs;
}

void OverlayDestroyFile(void *data) {
	auto file = static_cast<duckdb_v2_file_handle>(data);
	duckdb_v2_file_destroy(&file);
}

void OverlayOpen(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_open_info_handle open_info,
                 duckdb_v2_error_info_handle *err) {
	auto &overlay = OverlayOf(info, err);
	duckdb_v2_str path {nullptr, 0};
	duckdb_v2_virtual_file_open_info_get_path(open_info, &path, err);
	duckdb_v2_context_handle context = nullptr;
	duckdb_v2_virtual_file_open_info_get_context(open_info, &context, err);
	// Forward the request as received: same flags, same values, path with the overlay's prefix stripped.
	duckdb_v2_file_open_options_handle options = nullptr;
	if (duckdb_v2_virtual_file_open_info_get_options(open_info, &options, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_file_handle file = nullptr;
	auto rc =
	    duckdb_v2_file_system_open(DelegateFs(info, context, err), Convert(Underneath(path)), options, &file, err);
	duckdb_v2_file_open_options_destroy(&options);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_opaque data {file, OverlayDestroyFile, nullptr};
	duckdb_v2_virtual_file_open_info_set_file_data(open_info, &data, err);
	overlay.opens++;
}

duckdb_v2_file_handle OverlayFile(duckdb_v2_virtual_file_info_handle file, duckdb_v2_error_info_handle *err) {
	void *data = nullptr;
	duckdb_v2_virtual_file_info_get_file_data(file, &data, err);
	return static_cast<duckdb_v2_file_handle>(data);
}

void OverlayReadAt(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle file,
                   void *buffer, idx_t buffer_size, idx_t location, idx_t *bytes_read,
                   duckdb_v2_error_info_handle *err) {
	OverlayOf(info, err).reads++;
	// The engine's offset read is all or nothing; clamp to the size to keep pread semantics.
	auto underneath = OverlayFile(file, err);
	idx_t size = 0;
	if (duckdb_v2_file_size(underneath, &size, err) != DUCKDB_V2_ERROR_NONE || location >= size) {
		return;
	}
	auto count = std::min<idx_t>(buffer_size, size - location);
	if (duckdb_v2_file_read_at(underneath, buffer, count, location, err) == DUCKDB_V2_ERROR_NONE) {
		*bytes_read = count;
	}
}

void OverlayStat(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_info_handle file,
                 duckdb_v2_file_stat_handle stat, duckdb_v2_error_info_handle *err) {
	duckdb_v2_file_stat_handle underneath = nullptr;
	if (duckdb_v2_file_get_stat(OverlayFile(file, err), &underneath, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	idx_t size = 0;
	bool known = false;
	duckdb_v2_file_stat_get_size(underneath, &size, &known, err);
	if (known) {
		duckdb_v2_file_stat_set_size(stat, size, err);
	}
	duckdb_v2_file_stat_destroy(&underneath);
}

void OverlayStatPath(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_path_info_handle path_info,
                     duckdb_v2_file_stat_handle stat, duckdb_v2_error_info_handle *err) {
	OverlayOf(info, err).stats++;
	duckdb_v2_str path {nullptr, 0};
	duckdb_v2_virtual_file_path_info_get_path(path_info, &path, err);
	duckdb_v2_context_handle context = nullptr;
	duckdb_v2_virtual_file_path_info_get_context(path_info, &context, err);
	duckdb_v2_file_stat_handle underneath = nullptr;
	if (duckdb_v2_file_system_stat(DelegateFs(info, context, err), Convert(Underneath(path)), &underneath, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return;
	}
	DUCKDB_V2_FILE_TYPE type = DUCKDB_V2_FILE_TYPE_INVALID;
	duckdb_v2_file_stat_get_type(underneath, &type, err);
	duckdb_v2_file_stat_set_type(stat, type, err);
	// What the file system underneath knew by path comes through.
	idx_t size = 0;
	bool known = false;
	duckdb_v2_file_stat_get_size(underneath, &size, &known, err);
	if (known) {
		duckdb_v2_file_stat_set_size(stat, size, err);
	}
	duckdb_v2_file_stat_destroy(&underneath);
}

void OverlayList(duckdb_v2_virtual_file_system_info_handle info, duckdb_v2_virtual_file_path_info_handle path_info,
                 duckdb_v2_file_listing_handle list, duckdb_v2_error_info_handle *err) {
	OverlayOf(info, err).lists++;
	duckdb_v2_str path {nullptr, 0};
	duckdb_v2_virtual_file_path_info_get_path(path_info, &path, err);
	duckdb_v2_context_handle context = nullptr;
	duckdb_v2_virtual_file_path_info_get_context(path_info, &context, err);
	duckdb_v2_file_listing_handle underneath = nullptr;
	if (duckdb_v2_file_system_list(DelegateFs(info, context, err), Convert(Underneath(path)), &underneath, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return;
	}
	idx_t count = 0;
	duckdb_v2_file_listing_get_entry_count(underneath, &count, err);
	for (idx_t i = 0; i < count; i++) {
		duckdb_v2_str name {nullptr, 0};
		DUCKDB_V2_FILE_TYPE type = DUCKDB_V2_FILE_TYPE_INVALID;
		duckdb_v2_file_listing_get_entry_path(underneath, i, &name, err);
		duckdb_v2_file_listing_get_entry_type(underneath, i, &type, err);
		duckdb_v2_file_listing_add_entry(list, name, type, nullptr, err);
	}
	duckdb_v2_file_listing_destroy(&underneath);
}

void RegisterOverlay(duckdb_v2_connection_handle conn, Overlay &overlay) {
	duckdb_v2_virtual_file_system_handle vfs = nullptr;
	REQUIRE(duckdb_v2_virtual_file_system_create_with_connection(conn, &vfs, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_name(vfs, Convert("cached"), nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_opaque user_data {&overlay, nullptr, nullptr};
	REQUIRE(duckdb_v2_virtual_file_system_set_user_data(vfs, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_add_prefix(vfs, Convert(OVERLAY_SCHEME), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_open_callback(vfs, OverlayOpen, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_read_at_callback(vfs, OverlayReadAt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_stat_callback(vfs, OverlayStat, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_stat_path_callback(vfs, OverlayStatPath, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_set_list_callback(vfs, OverlayList, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_virtual_file_system_register(vfs, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_virtual_file_system_destroy(&vfs);
}

} // namespace

TEST_CASE("V2 virtual file system: an overlay delegates to the file system underneath", "[capi_v2][vfs]") {
	EnvFixture fx;
	MemFs mem;
	mem.files["mem://data/a.csv"] = CSV_A;
	mem.files["mem://data/b.csv"] = CSV_B;
	RegisterMemFs(fx.conn, mem);
	Overlay overlay;
	RegisterOverlay(fx.conn, overlay);

	// A single file: opened through the overlay, served by the in-memory file system underneath.
	auto rows = VfsQueryStrings(fx.conn, "SELECT s FROM read_csv('cached+mem://data/a.csv') ORDER BY i");
	REQUIRE(rows == std::vector<std::string> {"one", "two"});
	REQUIRE(overlay.opens > 0);
	REQUIRE(overlay.reads > 0);
	REQUIRE(mem.opens > 0);
	// The request was forwarded as received: the file underneath was opened for reading, not writing.
	REQUIRE(!mem.last_open_for_write);
	REQUIRE(mem.last_open_path == "mem://data/a.csv");

	// Listings and stats delegate too, so globs through the overlay expand underneath.
	auto files = VfsQueryStrings(fx.conn, "SELECT file FROM glob('cached+mem://data/*.csv')");
	REQUIRE(files == std::vector<std::string> {"cached+mem://data/a.csv", "cached+mem://data/b.csv"});
	REQUIRE(overlay.lists > 0);
	auto total = VfsQueryStrings(fx.conn, "SELECT count(*) FROM read_csv('cached+mem://data/*.csv')");
	REQUIRE(total == std::vector<std::string> {"3"});

	// A stat by path routes to the overlay, whose stat path passes on what the file system underneath knew.
	auto fs = VfsEngineFs(fx.conn);
	duckdb_v2_file_stat_handle stat = nullptr;
	REQUIRE(duckdb_v2_file_system_stat(fs, Convert("cached+mem://data/b.csv"), &stat, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_FILE_TYPE type = DUCKDB_V2_FILE_TYPE_INVALID;
	REQUIRE(duckdb_v2_file_stat_get_type(stat, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(type == DUCKDB_V2_FILE_TYPE_REGULAR);
	idx_t size = 0;
	bool known = false;
	REQUIRE(duckdb_v2_file_stat_get_size(stat, &size, &known, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(known);
	REQUIRE(size == std::strlen(CSV_B));
	duckdb_v2_file_stat_destroy(&stat);
	REQUIRE(overlay.stats > 0);
	// And a path that is not there says so, as a result.
	REQUIRE(duckdb_v2_file_system_stat(fs, Convert("cached+mem://data/missing.csv"), &stat, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_stat_get_type(stat, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(type == DUCKDB_V2_FILE_TYPE_INVALID);
	duckdb_v2_file_stat_destroy(&stat);

	// Every file the overlay opened underneath was closed with it.
	REQUIRE(mem.opens == mem.closes);
	REQUIRE(mem.opens == mem.file_data_destroyed);
}

// ---------------------------------------------------------------------------
// Consumer-side path operations against the local file system
// ---------------------------------------------------------------------------

TEST_CASE("V2 file system: path operations on the local file system", "[capi_v2][vfs][file_system]") {
	EnvFixture fx;
	auto fs = VfsEngineFs(fx.conn);
	auto root = duckdb::TestCreatePath("v2_fs_paths");
	duckdb_v2_file_system_remove_directory(fs, Convert(root), nullptr);

	// Nothing there yet: a stat says so as a result, and listing a directory that does not exist is an error.
	duckdb_v2_file_stat_handle stat = nullptr;
	REQUIRE(duckdb_v2_file_system_stat(fs, Convert(root), &stat, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_FILE_TYPE type = DUCKDB_V2_FILE_TYPE_REGULAR;
	REQUIRE(duckdb_v2_file_stat_get_type(stat, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(type == DUCKDB_V2_FILE_TYPE_INVALID);
	duckdb_v2_file_stat_destroy(&stat);
	REQUIRE(stat == nullptr);
	duckdb_v2_file_listing_handle listing = nullptr;
	REQUIRE(duckdb_v2_file_system_list(fs, Convert(root), &listing, nullptr) != DUCKDB_V2_ERROR_NONE);

	// Create a directory tree and a file in it.
	REQUIRE(duckdb_v2_file_system_create_directory(fs, Convert(root + "/sub"), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_system_stat(fs, Convert(root + "/sub"), &stat, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_stat_get_type(stat, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(type == DUCKDB_V2_FILE_TYPE_DIRECTORY);
	duckdb_v2_file_stat_destroy(&stat);
	idx_t count = 0;

	auto file_path = root + "/sub/data.txt";
	{
		auto handle = VfsOpen(fs, file_path, {DUCKDB_V2_FILE_FLAG_WRITE, DUCKDB_V2_FILE_FLAG_CREATE_NEW});
		idx_t written = 0;
		REQUIRE(duckdb_v2_file_write(handle, "hello", 5, &written, nullptr) == DUCKDB_V2_ERROR_NONE);
		// An open file reports its size, and on local disk its modification time.
		REQUIRE(duckdb_v2_file_get_stat(handle, &stat, nullptr) == DUCKDB_V2_ERROR_NONE);
		idx_t size = 0;
		bool known = false;
		REQUIRE(duckdb_v2_file_stat_get_size(stat, &size, &known, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(known);
		REQUIRE(size == 5);
		int64_t modified = 0;
		REQUIRE(duckdb_v2_file_stat_get_last_modified(stat, &modified, &known, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(known);
		// Whether a version tag is reported is the file system's business; the call itself must work.
		duckdb_v2_str tag {nullptr, 0};
		REQUIRE(duckdb_v2_file_stat_get_version_tag(stat, &tag, &known, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_file_stat_destroy(&stat);
		duckdb_v2_file_destroy(&handle);
	}

	// A stat by path sees a regular file, and on local disk knows its size without opening it.
	REQUIRE(duckdb_v2_file_system_stat(fs, Convert(file_path), &stat, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_stat_get_type(stat, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(type == DUCKDB_V2_FILE_TYPE_REGULAR);
	idx_t stat_size = 0;
	bool stat_size_known = false;
	REQUIRE(duckdb_v2_file_stat_get_size(stat, &stat_size, &stat_size_known, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stat_size_known);
	REQUIRE(stat_size == 5);
	duckdb_v2_file_stat_destroy(&stat);

	// Listing the directory yields the file by name, with its size.
	REQUIRE(duckdb_v2_file_system_list(fs, Convert(root + "/sub"), &listing, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_listing_get_entry_count(listing, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 1);
	duckdb_v2_str name {nullptr, 0};
	REQUIRE(duckdb_v2_file_listing_get_entry_path(listing, 0, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(name == "data.txt");
	REQUIRE(duckdb_v2_file_listing_get_entry_type(listing, 0, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(type == DUCKDB_V2_FILE_TYPE_REGULAR);
	duckdb_v2_file_stat_handle entry_stat = nullptr;
	REQUIRE(duckdb_v2_file_listing_get_entry_stat(listing, 0, &entry_stat, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t entry_size = 0;
	bool entry_size_known = false;
	REQUIRE(duckdb_v2_file_stat_get_size(entry_stat, &entry_size, &entry_size_known, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(entry_size_known);
	REQUIRE(entry_size == 5);
	REQUIRE(duckdb_v2_file_listing_get_entry_path(listing, 1, &name, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_file_listing_destroy(&listing);
	REQUIRE(listing == nullptr);

	// Globbing yields full paths; nothing matching is an empty listing.
	REQUIRE(duckdb_v2_file_system_glob(fs, Convert(root + "/*/*.txt"), &listing, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_listing_get_entry_count(listing, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 1);
	REQUIRE(duckdb_v2_file_listing_get_entry_path(listing, 0, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Convert(name).find("data.txt") != std::string::npos);
	duckdb_v2_file_listing_destroy(&listing);
	REQUIRE(duckdb_v2_file_system_glob(fs, Convert(root + "/*/*.csv"), &listing, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_listing_get_entry_count(listing, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);
	duckdb_v2_file_listing_destroy(&listing);

	// Move, then remove.
	auto moved_path = root + "/moved.txt";
	REQUIRE(duckdb_v2_file_system_move(fs, Convert(file_path), Convert(moved_path), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_system_glob(fs, Convert(root + "/sub/*"), &listing, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_listing_get_entry_count(listing, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 0);
	duckdb_v2_file_listing_destroy(&listing);
	REQUIRE(duckdb_v2_file_system_remove_file(fs, Convert(moved_path), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_system_remove_file(fs, Convert(moved_path), nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_system_remove_directory(fs, Convert(root), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_system_list(fs, Convert(root), &listing, nullptr) != DUCKDB_V2_ERROR_NONE);

	// Null-safe destroys, null arguments reported.
	REQUIRE(duckdb_v2_file_stat_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_listing_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_file_system_list(nullptr, Convert(root), &listing, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_file_system_stat(nullptr, Convert(root), &stat, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

} // namespace test_capi_v2
