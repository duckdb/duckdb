#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb_httplib_openssl {
struct Response;
}

namespace duckdb {

using HeaderMap = unordered_map<string, string>;

struct ResponseWrapper { /* avoid including httplib in header */
public:
	ResponseWrapper(duckdb_httplib_openssl::Response &res);
	int code;
	string error;
	HeaderMap headers;
};

class HTTPFileHandle : public FileHandle {
public:
	HTTPFileHandle(FileSystem &fs, std::string path);
	// This two-phase construction allows subclasses more flexible setup.
	virtual void InitializeMetadata();

	idx_t length;
	time_t last_modified;

	std::unique_ptr<data_t[]> buffer;
	constexpr static idx_t BUFFER_LEN = 1000000;
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;

public:
	void Close() override {
	}
};

class HTTPFileSystem : public FileSystem {
public:
	std::unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = DEFAULT_LOCK,
	                                     FileCompressionType compression = DEFAULT_COMPRESSION,
	                                     FileOpener *opener = nullptr) override final;

	std::vector<std::string> Glob(const std::string &path) override {
		return {path}; // FIXME
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

	virtual unique_ptr<ResponseWrapper> Request(FileHandle &handle, string url, string method,
	                                            HeaderMap header_map = {}, idx_t file_offset = 0,
	                                            char *buffer_out = nullptr, idx_t buffer_len = 0);

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	int64_t GetFileSize(FileHandle &handle) override;

	time_t GetLastModifiedTime(FileHandle &handle) override;

	bool FileExists(const string &filename) override;

	static void Verify();

	bool CanSeek() override {
		return true;
	}

	void Seek(FileHandle &handle, idx_t location) override;

	bool CanHandleFile(const string &fpath) override;
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}

	std::string GetName() const override {
		return "HTTPFileSystem";
	}

protected:
	virtual std::unique_ptr<HTTPFileHandle> CreateHandle(const string &path, uint8_t flags, FileLockType lock,
	                                                     FileCompressionType compression, FileOpener *opener);
};

} // namespace duckdb
