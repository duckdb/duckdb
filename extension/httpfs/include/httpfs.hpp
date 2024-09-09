#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/file_system.hpp"
#include "http_state.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_data.hpp"
#include "http_metadata_cache.hpp"

namespace duckdb_httplib_openssl {
struct Response;
class Result;
class Client;
namespace detail {
struct ci;
}
using Headers = std::multimap<std::string, std::string, duckdb_httplib_openssl::detail::ci>;
} // namespace duckdb_httplib_openssl

namespace duckdb {

class HTTPLogger;

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
	static constexpr bool DEFAULT_KEEP_ALIVE = true;
	static constexpr bool DEFAULT_ENABLE_SERVER_CERT_VERIFICATION = false;
	static constexpr uint64_t DEFAULT_HF_MAX_PER_PAGE = 0;

	uint64_t timeout = DEFAULT_TIMEOUT;
	uint64_t retries = DEFAULT_RETRIES;
	uint64_t retry_wait_ms = DEFAULT_RETRY_WAIT_MS;
	float retry_backoff = DEFAULT_RETRY_BACKOFF;
	bool force_download = DEFAULT_FORCE_DOWNLOAD;
	bool keep_alive = DEFAULT_KEEP_ALIVE;
	bool enable_server_cert_verification = DEFAULT_ENABLE_SERVER_CERT_VERIFICATION;
	idx_t hf_max_per_page = DEFAULT_HF_MAX_PER_PAGE;

	string ca_cert_file;
	string http_proxy;
	idx_t http_proxy_port;
	string http_proxy_username;
	string http_proxy_password;
	string bearer_token;
	unordered_map<string, string> extra_headers;

	static HTTPParams ReadFrom(optional_ptr<FileOpener> opener, optional_ptr<FileOpenerInfo> info);
};

class HTTPClientCache {
public:
	//! Get a client from the client cache
	unique_ptr<duckdb_httplib_openssl::Client> GetClient();
	//! Store a client in the cache for reuse
	void StoreClient(unique_ptr<duckdb_httplib_openssl::Client> client);

protected:
	//! The cached clients
	vector<unique_ptr<duckdb_httplib_openssl::Client>> clients;
	//! Lock to fetch a client
	mutex lock;
};

class HTTPFileHandle : public FileHandle {
public:
	HTTPFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags, const HTTPParams &params);
	~HTTPFileHandle() override;
	// This two-phase construction allows subclasses more flexible setup.
	virtual void Initialize(optional_ptr<FileOpener> opener);

	// We keep an http client stored for connection reuse with keep-alive headers
	HTTPClientCache client_cache;

	optional_ptr<HTTPLogger> http_logger;

	const HTTPParams http_params;

	// File handle info
	FileOpenFlags flags;
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

	void AddHeaders(HeaderMap &map);

	// Get a Client to run requests over
	unique_ptr<duckdb_httplib_openssl::Client> GetClient(optional_ptr<ClientContext> client_context);
	// Return the client for re-use
	void StoreClient(unique_ptr<duckdb_httplib_openssl::Client> client);

public:
	void Close() override {
	}

protected:
	//! Create a new Client
	virtual unique_ptr<duckdb_httplib_openssl::Client> CreateClient(optional_ptr<ClientContext> client_context);
};

class HTTPFileSystem : public FileSystem {
public:
	static duckdb::unique_ptr<duckdb_httplib_openssl::Client>
	GetClient(const HTTPParams &http_params, const char *proto_host_port, optional_ptr<HTTPFileHandle> hfs);
	static void ParseUrl(string &url, string &path_out, string &proto_host_port_out);
	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener = nullptr) final;
	static duckdb::unique_ptr<duckdb_httplib_openssl::Headers> InitializeHeaders(HeaderMap &header_map,
	                                                                             const HTTPParams &http_params);

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
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override;
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool CanHandleFile(const string &fpath) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override {
		return false;
	}
	string GetName() const override {
		return "HTTPFileSystem";
	}
	string PathSeparator(const string &path) override {
		return "/";
	}
	static void Verify();

	optional_ptr<HTTPMetadataCache> GetGlobalCache();

protected:
	virtual duckdb::unique_ptr<HTTPFileHandle> CreateHandle(const string &path, FileOpenFlags flags,
	                                                        optional_ptr<FileOpener> opener);

	static duckdb::unique_ptr<ResponseWrapper>
	RunRequestWithRetry(const std::function<duckdb_httplib_openssl::Result(void)> &request, string &url, string method,
	                    const HTTPParams &params, const std::function<void(void)> &retry_cb = {});

private:
	// Global cache
	mutex global_cache_lock;
	duckdb::unique_ptr<HTTPMetadataCache> global_metadata_cache;
};

} // namespace duckdb
