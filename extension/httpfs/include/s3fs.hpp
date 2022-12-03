#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "httpfs.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

#include <condition_variable>
#include <iostream>

namespace duckdb {

struct AWSEnvironmentCredentialsProvider {
	static constexpr const char *REGION_ENV_VAR = "AWS_DEFAULT_REGION";
	static constexpr const char *ACCESS_KEY_ENV_VAR = "AWS_ACCESS_KEY_ID";
	static constexpr const char *SECRET_KEY_ENV_VAR = "AWS_SECRET_ACCESS_KEY";
	static constexpr const char *SESSION_TOKEN_ENV_VAR = "AWS_SESSION_TOKEN";

	explicit AWSEnvironmentCredentialsProvider(DBConfig &config) : config(config) {};

	DBConfig &config;

	void SetExtensionOptionValue(string key, const char *env_var);
	void SetAll();
};

struct S3AuthParams {
	string region;
	string access_key_id;
	string secret_access_key;
	string session_token;
	string endpoint;
	string url_style;
	bool use_ssl;

	static S3AuthParams ReadFrom(FileOpener *opener);
};

struct ParsedS3Url {
	const string http_proto;
	const string host;
	const string bucket;
	const string path;
	const string query_param;
};

struct S3ConfigParams {
	static constexpr uint64_t DEFAULT_MAX_FILESIZE = 800000000000; // 800GB
	static constexpr uint64_t DEFAULT_MAX_PARTS_PER_FILE = 10000;  // AWS DEFAULT
	static constexpr uint64_t DEFAULT_MAX_UPLOAD_THREADS = 50;

	uint64_t max_file_size;
	uint64_t max_parts_per_file;
	uint64_t max_upload_threads;

	static S3ConfigParams ReadFrom(FileOpener *opener);
};

class S3FileSystem;

// Holds the buffered data for 1 part of an S3 Multipart upload
class S3WriteBuffer {
public:
	explicit S3WriteBuffer(idx_t buffer_start, size_t buffer_size, BufferHandle buffer_p)
	    : idx(0), buffer_start(buffer_start), buffer(move(buffer_p)) {
		buffer_end = buffer_start + buffer_size;
		part_no = buffer_start / buffer_size;
		uploading = false;
	}

	void *Ptr() {
		return buffer.Ptr();
	}

	// The S3 multipart part number. Note that internally we start at 0 but AWS S3 starts at 1
	idx_t part_no;

	idx_t idx;
	idx_t buffer_start;
	idx_t buffer_end;
	BufferHandle buffer;
	atomic<bool> uploading;
};

class S3FileHandle : public HTTPFileHandle {
	friend class S3FileSystem;

public:
	S3FileHandle(FileSystem &fs, string path_p, const string &stripped_path_p, uint8_t flags,
	             const HTTPParams &http_params, const S3AuthParams &auth_params_p,
	             const S3ConfigParams &config_params_p)
	    : HTTPFileHandle(fs, move(path_p), flags, http_params), auth_params(auth_params_p),
	      config_params(config_params_p), stripped_path(stripped_path_p) {

		if (flags & FileFlags::FILE_FLAGS_WRITE && flags & FileFlags::FILE_FLAGS_READ) {
			throw NotImplementedException("Cannot open an HTTP file for both reading and writing");
		} else if (flags & FileFlags::FILE_FLAGS_APPEND) {
			throw NotImplementedException("Cannot open an HTTP file for appending");
		}
	}
	S3AuthParams auth_params;
	const S3ConfigParams config_params;
	string stripped_path;

public:
	void Close() override;
	unique_ptr<ResponseWrapper> Initialize() override;

protected:
	string multipart_upload_id;
	size_t part_size;

	mutex write_buffers_lock;
	unordered_map<uint16_t, shared_ptr<S3WriteBuffer>> write_buffers;

	mutex uploads_in_progress_lock;
	std::condition_variable uploads_in_progress_cv;
	atomic<uint16_t> uploads_in_progress;

	// Etags are stored for each part
	mutex part_etags_lock;
	unordered_map<uint16_t, string> part_etags;

	atomic<uint16_t> parts_uploaded;
	bool upload_finalized;

	void InitializeClient() override;
};

class S3FileSystem : public HTTPFileSystem {
public:
	explicit S3FileSystem(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
	}

	// Global limits to write buffers
	mutex buffers_available_lock;
	std::condition_variable buffers_available_cv;
	atomic<uint16_t> buffers_available;
	atomic<uint16_t> threads_waiting_for_memory = {0};

	BufferManager &buffer_manager;

public:
	// HTTP Requests
	unique_ptr<ResponseWrapper> PostRequest(FileHandle &handle, string url, HeaderMap header_map,
	                                        unique_ptr<char[]> &buffer_out, idx_t &buffer_out_len, char *buffer_in,
	                                        idx_t buffer_in_len) override;
	unique_ptr<ResponseWrapper> PutRequest(FileHandle &handle, string url, HeaderMap header_map, char *buffer_in,
	                                       idx_t buffer_in_len) override;
	unique_ptr<ResponseWrapper> HeadRequest(FileHandle &handle, string url, HeaderMap header_map) override;
	unique_ptr<ResponseWrapper> GetRangeRequest(FileHandle &handle, string url, HeaderMap header_map, idx_t file_offset,
	                                            char *buffer_out, idx_t buffer_out_len) override;

	static void Verify();

	bool CanHandleFile(const string &fpath) override;
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	void FileSync(FileHandle &handle) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

	string InitializeMultipartUpload(S3FileHandle &file_handle);
	void FinalizeMultipartUpload(S3FileHandle &file_handle);

	void FlushAllBuffers(S3FileHandle &handle);

	void ReadQueryParams(const string &url_query_param, S3AuthParams &params);
	static ParsedS3Url S3UrlParse(string url, S3AuthParams &params);

	static string UrlEncode(const string &input, bool encode_slash = false);
	static string UrlDecode(string input);

	// Uploads the contents of write_buffer to S3.
	// Note: caller is responsible to not call this method twice on the same buffer
	static void UploadBuffer(S3FileHandle &file_handle, shared_ptr<S3WriteBuffer> write_buffer);

	vector<string> Glob(const string &glob_pattern, FileOpener *opener = nullptr) override;

protected:
	unique_ptr<HTTPFileHandle> CreateHandle(const string &path, const string &query_param, uint8_t flags,
	                                        FileLockType lock, FileCompressionType compression,
	                                        FileOpener *opener) override;

	void FlushBuffer(S3FileHandle &handle, shared_ptr<S3WriteBuffer> write_buffer);
	string GetPayloadHash(char *buffer, idx_t buffer_len);

	// helper for ReadQueryParams
	void GetQueryParam(const string &key, string &param, CPPHTTPLIB_NAMESPACE::Params &query_params);

	// Allocate an S3WriteBuffer
	// Note: call may block if no buffers are available or if the buffer manager fails to allocate more memory.
	shared_ptr<S3WriteBuffer> GetBuffer(S3FileHandle &file_handle, uint16_t write_buffer_idx);
};

// Helper class to do s3 ListObjectV2 api call https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
struct AWSListObjectV2 {
	static string Request(string &path, HTTPParams &http_params, S3AuthParams &s3_auth_params,
	                      string &continuation_token, bool use_delimiter = false);
	static void ParseKey(string &aws_response, vector<string> &result);
	static vector<string> ParseCommonPrefix(string &aws_response);
	static string ParseContinuationToken(string &aws_response);
};
} // namespace duckdb
