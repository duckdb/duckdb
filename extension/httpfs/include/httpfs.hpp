#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/list.hpp"

namespace duckdb_httplib_openssl {
struct Response;
class Client;
} // namespace duckdb_httplib_openssl

namespace duckdb {

using HeaderMap = case_insensitive_map_t<string>;

// avoid including httplib in header
struct ResponseWrapper {
public:
	explicit ResponseWrapper(duckdb_httplib_openssl::Response &res);
	int code;
	string error;
	HeaderMap headers;
};

struct HTTPParams {
	static constexpr uint64_t DEFAULT_TIMEOUT = 30000;            // 30 sec
	static constexpr uint64_t DEFAULT_METADATA_CACHE_MAX_AGE = 0; // disabled

	uint64_t timeout;
	uint64_t metadata_cache_max_age;

	static HTTPParams ReadFrom(FileOpener *opener);
};

struct FileHandleCacheValue {
	string path;
	idx_t length;
	time_t last_modified;
	milliseconds cache_time;
};

class HTTPFileCache {
public:
	// Note: duplicate inserts leave old deque entry dangling, which will eventually be cleared by PruneExpired
	void Insert(string path, idx_t length, time_t last_modified) {
		lock_guard<mutex> parallel_lock(lock);
		empty = false;
		milliseconds current_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
		deque.push_back({path, length, last_modified, current_time});
		map[path] = --deque.end();
	}

	void Erase(string path) {
		lock_guard<mutex> parallel_lock(lock);
		EraseInternal(path);
	}

	bool Find(string path, FileHandleCacheValue &ret_val, uint64_t max_age) {
		lock_guard<mutex> parallel_lock(lock);
		auto lookup = map.find(path);
		bool found = lookup != map.end();
		if (found) {
			milliseconds current_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
			if (current_time - lookup->second->cache_time <= milliseconds(max_age)) {
				ret_val = *lookup->second;
				return true;
			}
			EraseInternal(path);
		}

		return false;
	}

	void PruneExpired(uint64_t max_age) {
		lock_guard<mutex> parallel_lock(lock);
		milliseconds current_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
		for (auto const &i : deque) {
			if (current_time - i.cache_time > milliseconds(max_age)) {
				map.erase(i.path);
				deque.pop_front();
			} else {
				return;
			}
		}

		empty = true;
	}

	// Note: since we access empty without lock, in some edge cases this may not actually prune all
	void PruneAll() {
		if (empty) {
			return;
		}
		lock_guard<mutex> parallel_lock(lock);
		deque.clear();
		map.clear();
		empty = true;
	}

protected:
	using list_position = list<FileHandleCacheValue>::iterator;
	using file_handle_map_t = unordered_map<string, list_position>;

	mutex lock;
	file_handle_map_t file_handle_cache;
	file_handle_map_t map;
	list<FileHandleCacheValue> deque;

	bool empty = true;

	void EraseInternal(string path) {
		auto lookup = map.find(path);
		if (lookup != map.end()) {
			deque.erase(lookup->second);
		}
		map.erase(path);
	}
};

class HTTPFileHandle : public FileHandle {
public:
	HTTPFileHandle(FileSystem &fs, string path, uint8_t flags, const HTTPParams &params);
	~HTTPFileHandle() override;
	// This two-phase construction allows subclasses more flexible setup.
	virtual void Initialize();

	// We keep an http client stored for connection reuse with keep-alive headers
	unique_ptr<duckdb_httplib_openssl::Client> http_client;

	const HTTPParams http_params;

	// File handle info
	uint8_t flags;
	idx_t length;
	time_t last_modified;

	// Read info
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;

	// Read buffer
	unique_ptr<data_t[]> read_buffer;
	constexpr static idx_t READ_BUFFER_LEN = 1000000;

public:
	void Close() override {
	}

protected:
	virtual void InitializeClient();
};

class HTTPFileSystem : public FileSystem {
public:
	static unique_ptr<duckdb_httplib_openssl::Client> GetClient(const HTTPParams &http_params,
	                                                            const char *proto_host_port);
	static void ParseUrl(string &url, string &path_out, string &proto_host_port_out);
	unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = DEFAULT_LOCK,
	                                FileCompressionType compression = DEFAULT_COMPRESSION,
	                                FileOpener *opener = nullptr) final;

	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override {
		return {path}; // FIXME
	}

	// HTTP Requests
	virtual unique_ptr<ResponseWrapper> PutRequest(FileHandle &handle, string url, HeaderMap header_map,
	                                               char *buffer_in, idx_t buffer_in_len);
	virtual unique_ptr<ResponseWrapper> HeadRequest(FileHandle &handle, string url, HeaderMap header_map);
	// Get Request with range parameter that GETs exactly buffer_out_len bytes from the url
	virtual unique_ptr<ResponseWrapper> GetRangeRequest(FileHandle &handle, string url, HeaderMap header_map,
	                                                    idx_t file_offset, char *buffer_out, idx_t buffer_out_len);
	// Post Request that can handle variable sized responses without a content-length header (needed for s3 multipart)
	virtual unique_ptr<ResponseWrapper> PostRequest(FileHandle &handle, string url, HeaderMap header_map,
	                                                unique_ptr<char[]> &buffer_out, idx_t &buffer_out_len,
	                                                char *buffer_in, idx_t buffer_in_len);

	// FS methods
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void FileSync(FileHandle &handle) override;
	int64_t GetFileSize(FileHandle &handle) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	bool FileExists(const string &filename) override;
	void Seek(FileHandle &handle, idx_t location) override;
	bool CanHandleFile(const string &fpath) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	string GetName() const override {
		return "HTTPFileSystem";
	}

	static void Verify();

	HTTPFileCache metadata_cache;

protected:
	virtual unique_ptr<HTTPFileHandle> CreateHandle(const string &path, const string &query_param, uint8_t flags,
	                                                FileLockType lock, FileCompressionType compression,
	                                                FileOpener *opener);
};

} // namespace duckdb
