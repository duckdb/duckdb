#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "http_metadata_cache.hpp"
#include <iostream>

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

class HTTPFileHandle : public FileHandle {
public:
	HTTPFileHandle(FileSystem &fs, string path, uint8_t flags, const HTTPParams &params);
	~HTTPFileHandle() override;
	// This two-phase construction allows subclasses more flexible setup.
	virtual void Initialize(FileOpener *opener);

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

struct HTTPFileSystemStats {
	atomic<idx_t> head_count {0};
	atomic<idx_t> get_count {0};
	atomic<idx_t> total_bytes_received {0};
};

class HTTPFileSystem : public FileSystem {
public:
	~HTTPFileSystem() {
		std::cout << "\nHTTPFS stats\n";
		std::cout << " - GET: " << std::to_string(stats.get_count) << "\n";
		std::cout << " - HEAD: " << std::to_string(stats.head_count) << "\n";
		std::cout << " - total bytes received: " << std::to_string(stats.total_bytes_received) << "\n";
	}
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

	HTTPFileSystemStats stats;
	bool enable_stats = true;

	// Global cache
	unique_ptr<HTTPMetadataCache> global_metadata_cache;

protected:
	virtual unique_ptr<HTTPFileHandle> CreateHandle(const string &path, const string &query_param, uint8_t flags,
	                                                FileLockType lock, FileCompressionType compression,
	                                                FileOpener *opener);
};

} // namespace duckdb
