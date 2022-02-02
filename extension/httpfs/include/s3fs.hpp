#pragma once

#include "httpfs.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/mutex.hpp"

#include <atomic>
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

class S3FileSystem;

// Holds the buffered data for 1 part of an S3 Multipart upload
class S3WriteBuffer {
public:
	explicit S3WriteBuffer(idx_t buffer_start) : idx(0), buffer_start(buffer_start) {
		buffer_end = buffer_start + BUFFER_LEN;
		part_no = buffer_start / BUFFER_LEN;
		buffer = std::unique_ptr<data_t[]>(new data_t[BUFFER_LEN]);
		uploading = false;
	}

	// The S3 multipart part number.
	// Note that we internally start at 0 but AWS S3 starts at 1
	idx_t part_no;

	// TODO make configurable
	//	constexpr static idx_t BUFFER_LEN = 1 << 27; // 128 MiB
	constexpr static idx_t BUFFER_LEN = 6000000; // 128 MiB

	idx_t idx;
	idx_t buffer_start;
	idx_t buffer_end;

	std::unique_ptr<data_t[]> buffer;
	std::atomic<bool> uploading;
};

class S3FileHandle : public HTTPFileHandle {
friend class S3FileSystem;

public:
	S3FileHandle(FileSystem &fs, std::string path, uint8_t flags, const S3AuthParams &auth_params_p)
	    : HTTPFileHandle(fs, std::move(path), flags), auth_params(auth_params_p) {

		if (flags & FileFlags::FILE_FLAGS_WRITE && flags & FileFlags::FILE_FLAGS_READ) {
			throw NotImplementedException("Cannot open an HTTP file for both reading and writing");
		} else if (flags & FileFlags::FILE_FLAGS_APPEND) {
			throw NotImplementedException("Cannot open an HTTP file for appending");
		} else if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
			throw NotImplementedException("Cannot open an HTTP file with Direct I/O flag");
		}
	}
	const S3AuthParams auth_params;

	void Close() override;
	unique_ptr<ResponseWrapper> Initialize() override;

protected:
	constexpr static idx_t WRITE_MAX_UPLOADS = 10;
	constexpr static idx_t MAX_MEMORY_USAGE = WRITE_MAX_UPLOADS * S3WriteBuffer::BUFFER_LEN;
	constexpr static idx_t MAX_FILE_SIZE = 10000 * S3WriteBuffer::BUFFER_LEN;

	string multipart_upload_id;

	std::mutex write_buffers_mutex;
	unordered_map<uint16_t, std::shared_ptr<S3WriteBuffer>> write_buffers;
	//	std::vector<std::unique_ptr<S3WriteBuffer>> write_buffers_unused; // Todo make recycling bin for write buffers

	std::mutex buffers_available_mutex;
	std::condition_variable buffers_available_cv;
	std::atomic<uint16_t> buffers_available;

	std::mutex uploads_in_progress_mutex;
	std::condition_variable uploads_in_progress_cv;
	std::atomic<uint16_t> uploads_in_progress;

	// Etags are stored for each part
	std::mutex part_etags_mutex;
	std::unordered_map<uint16_t, string> part_etags;

	// For debugging
	std::atomic<uint16_t> parts_uploaded;

	// This variable is set after the upload is finalized, it will prevent any further writes.
	bool upload_finalized;
};

class S3FileSystem : public HTTPFileSystem {
public:
	S3FileSystem() {
	}

	unique_ptr<ResponseWrapper> PostRequest(FileHandle &handle, string url,HeaderMap header_map, unique_ptr<char[]> &buffer_out, idx_t &buffer_out_len, char* buffer_in, idx_t buffer_in_len) override;
	unique_ptr<ResponseWrapper> PutRequest(FileHandle &handle, string url, HeaderMap header_map, char* buffer_in, idx_t buffer_in_len) override;
	unique_ptr<ResponseWrapper> HeadRequest(FileHandle &handle, string url,HeaderMap header_map) override;
	unique_ptr<ResponseWrapper> GetRangeRequest(FileHandle &handle, string url, HeaderMap header_map, idx_t file_offset, char *buffer_out, idx_t buffer_out_len) override;

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

	// Uploads the contents of write_buffer to S3.
	// Note: caller is responsible to not call this method twice on the same buffer
	static void UploadBuffer(S3FileHandle &file_handle, shared_ptr<S3WriteBuffer> write_buffer);

protected:
	std::unique_ptr<HTTPFileHandle> CreateHandle(const string &path, uint8_t flags, FileLockType lock,
	                                             FileCompressionType compression, FileOpener *opener) override;

	// TODO: allow resuming upload after flushing non-full buffer?
	void FlushBuffer(S3FileHandle &handle, std::shared_ptr<S3WriteBuffer> write_buffer);

	// Helper functions
	void S3UrlParse(FileHandle &handle, string url, string& host_out, string& http_host_out, string& path_out, string& query_param);
	string GetPayloadHash(char* buffer, idx_t buffer_len);

	// Allocate an S3WriteBuffer
	// Note: call will block if no buffers are available
	std::shared_ptr<S3WriteBuffer> GetBuffer(S3FileHandle &file_handle, uint16_t write_buffer_idx);
};
} // namespace duckdb
