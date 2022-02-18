#pragma once

#include "httpfs.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/atomic.hpp"

#include <condition_variable>
#include <iostream>

namespace duckdb {

struct S3AuthParams {
	const std::string region;
	const std::string access_key_id;
	const std::string secret_access_key;
	const std::string session_token;
	const std::string endpoint;

	static S3AuthParams ReadFrom(FileOpener *opener);
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
	explicit S3WriteBuffer(idx_t buffer_start, size_t buffer_size, unique_ptr<BufferHandle> &buffer)
	    : idx(0), buffer_start(buffer_start), buffer(std::move(buffer)) {
		buffer_end = buffer_start + buffer_size;
		part_no = buffer_start / buffer_size;
		uploading = false;
	}

	void *Ptr() {
		return buffer->Ptr();
	}

	// The S3 multipart part number. Note that internally we start at 0 but AWS S3 starts at 1
	idx_t part_no;

	idx_t idx;
	idx_t buffer_start;
	idx_t buffer_end;
	unique_ptr<BufferHandle> buffer;
	std::atomic<bool> uploading;
};

class S3FileHandle : public HTTPFileHandle {
	friend class S3FileSystem;

public:
	S3FileHandle(FileSystem &fs, std::string path, uint8_t flags, const HTTPParams &http_params,
	             const S3AuthParams &auth_params_p, const S3ConfigParams &config_params_p)
	    : HTTPFileHandle(fs, std::move(path), flags, http_params), auth_params(auth_params_p),
	      config_params(config_params_p) {

		if (flags & FileFlags::FILE_FLAGS_WRITE && flags & FileFlags::FILE_FLAGS_READ) {
			throw NotImplementedException("Cannot open an HTTP file for both reading and writing");
		} else if (flags & FileFlags::FILE_FLAGS_APPEND) {
			throw NotImplementedException("Cannot open an HTTP file for appending");
		} else if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
			throw NotImplementedException("Cannot open an HTTP file with Direct I/O flag");
		}
	}
	const S3AuthParams auth_params;
	const S3ConfigParams config_params;

	void Close() override;
	unique_ptr<ResponseWrapper> Initialize() override;

protected:
	string multipart_upload_id;
	size_t part_size;

	std::mutex write_buffers_lock;
	unordered_map<uint16_t, std::shared_ptr<S3WriteBuffer>> write_buffers;

	std::mutex uploads_in_progress_lock;
	std::condition_variable uploads_in_progress_cv;
	std::atomic<uint16_t> uploads_in_progress;

	// Etags are stored for each part
	std::mutex part_etags_lock;
	std::unordered_map<uint16_t, string> part_etags;

	std::atomic<uint16_t> parts_uploaded;
	bool upload_finalized;

	void InitializeClient() override;
};

class S3FileSystem : public HTTPFileSystem {
public:
	constexpr static int MULTIPART_UPLOAD_WAIT_BETWEEN_RETRIES_MS = 1000;

	// Global limits to write buffers
	std::mutex buffers_available_lock;
	std::condition_variable buffers_available_cv;
	std::atomic<uint16_t> buffers_available;
	std::atomic<uint16_t> threads_waiting_for_memory = {0};

	explicit S3FileSystem(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
	}

	BufferManager &buffer_manager;

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

	static void S3UrlParse(FileHandle &handle, string url, string &host_out, string &http_proto_out, string &path_out,
	                       string &query_param);

	// Uploads the contents of write_buffer to S3.
	// Note: caller is responsible to not call this method twice on the same buffer
	static void UploadBuffer(S3FileHandle &file_handle, shared_ptr<S3WriteBuffer> write_buffer);

protected:
	std::unique_ptr<HTTPFileHandle> CreateHandle(const string &path, uint8_t flags, FileLockType lock,
	                                             FileCompressionType compression, FileOpener *opener) override;

	void FlushBuffer(S3FileHandle &handle, std::shared_ptr<S3WriteBuffer> write_buffer);
	string GetPayloadHash(char *buffer, idx_t buffer_len);

	// Allocate an S3WriteBuffer
	// Note: call may block if no buffers are available or if the buffer manager fails to allocate more memory.
	std::shared_ptr<S3WriteBuffer> GetBuffer(S3FileHandle &file_handle, uint16_t write_buffer_idx);
};
} // namespace duckdb
