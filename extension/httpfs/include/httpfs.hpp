#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/http_state.hpp"
#include "duckdb/main/client_data.hpp"
#include "http_metadata_cache.hpp"

namespace duckdb_httplib_openssl {
struct Response;
class Client;
} // namespace duckdb_httplib_openssl

namespace duckdb {

using HeaderMap = case_insensitive_map_t<string>;

// avoid including httplib in header
struct ResponseWrapper {
public:
	explicit ResponseWrapper(duckdb_httplib_openssl::Response &res, string &original_url);
	int code;
	string error;
	HeaderMap headers;
	string http_url;
	string body;
};

struct HTTPParams {

	static constexpr uint64_t DEFAULT_TIMEOUT = 30000; // 30 sec
	static constexpr uint64_t DEFAULT_RETRIES = 3;
	static constexpr uint64_t DEFAULT_RETRY_WAIT_MS = 100;
	static constexpr float DEFAULT_RETRY_BACKOFF = 4;
	static constexpr bool DEFAULT_FORCE_DOWNLOAD = false;

	uint64_t timeout;
	uint64_t retries;
	uint64_t retry_wait_ms;
	float retry_backoff;
	bool force_download;

	static HTTPParams ReadFrom(FileOpener *opener);
};

class HTTPFileHandle : public FileHandle {
public:
	HTTPFileHandle(FileSystem &fs, string path, uint8_t flags, const HTTPParams &params);
	~HTTPFileHandle() override;
	// This two-phase construction allows subclasses more flexible setup.
	virtual void Initialize(FileOpener *opener);

	// We keep an http client stored for connection reuse with keep-alive headers
	duckdb::unique_ptr<duckdb_httplib_openssl::Client> http_client;

	const HTTPParams http_params;

	// File handle info
	uint8_t flags;
	idx_t length;
	time_t last_modified;

	// When using full file download, the full file will be written to a cached file handle
	unique_ptr<CachedFileHandle> cached_file_handle;

	// Read info
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;

	// Read buffer
	duckdb::unique_ptr<data_t[]> read_buffer;
	constexpr static idx_t READ_BUFFER_LEN = 1000000;

	shared_ptr<HTTPState> state;

public:
	void Close() override {
	}

protected:
	virtual void InitializeClient();
};

class HTTPFileSystem : public FileSystem {
public:
	static duckdb::unique_ptr<duckdb_httplib_openssl::Client> GetClient(const HTTPParams &http_params,
	                                                                    const char *proto_host_port);
	static void ParseUrl(string &url, string &path_out, string &proto_host_port_out);
	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = DEFAULT_LOCK,
	                                        FileCompressionType compression = DEFAULT_COMPRESSION,
	                                        FileOpener *opener = nullptr) final;

	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override {
		return {path}; // FIXME
	}

	// HTTP Requests
	virtual duckdb::unique_ptr<ResponseWrapper> HeadRequest(FileHandle &handle, string url, HeaderMap header_map);
	// Get Request with range parameter that GETs exactly buffer_out_len bytes from the url
	virtual duckdb::unique_ptr<ResponseWrapper> GetRangeRequest(FileHandle &handle, string url, HeaderMap header_map,
	                                                            idx_t file_offset, char *buffer_out,
	                                                            idx_t buffer_out_len);
	// Get Request without a range (i.e., downloads full file)
	virtual duckdb::unique_ptr<ResponseWrapper> GetRequest(FileHandle &handle, string url, HeaderMap header_map);
	// Post Request that can handle variable sized responses without a content-length header (needed for s3 multipart)
	virtual duckdb::unique_ptr<ResponseWrapper> PostRequest(FileHandle &handle, string url, HeaderMap header_map,
	                                                        duckdb::unique_ptr<char[]> &buffer_out,
	                                                        idx_t &buffer_out_len, char *buffer_in, idx_t buffer_in_len,
	                                                        string params = "");
	virtual duckdb::unique_ptr<ResponseWrapper> PutRequest(FileHandle &handle, string url, HeaderMap header_map,
	                                                       char *buffer_in, idx_t buffer_in_len, string params = "");

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
	idx_t SeekPosition(FileHandle &handle) override;
	bool CanHandleFile(const string &fpath) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	bool IsPipe(const string &filename) override {
		return false;
	}
	string GetName() const override {
		return "HTTPFileSystem";
	}
	string PathSeparator(const string &path) override {
		return "/";
	}
	static void Verify();

	// Global cache
	duckdb::unique_ptr<HTTPMetadataCache> global_metadata_cache;

protected:
	virtual duckdb::unique_ptr<HTTPFileHandle> CreateHandle(const string &path, uint8_t flags, FileLockType lock,
	                                                        FileCompressionType compression, FileOpener *opener);
};

} // namespace duckdb
