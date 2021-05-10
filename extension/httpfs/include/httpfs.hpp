#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace httplib {
struct Response;
}

namespace duckdb {

using HeaderMap = unordered_map<string, string>;

struct ResponseWrapper { /* avoid including httplib in header */
public:
	ResponseWrapper(httplib::Response &res);
	int code;
	string error;
	HeaderMap headers;
};

class HTTPFileHandle : public FileHandle {
public:
	HTTPFileHandle(FileSystem &fs, std::string path);

protected:
	void Close() override {
	}

private:
	virtual void IntializeMetadata();

public:
	idx_t length;
	time_t last_modified;

	std::unique_ptr<data_t[]> buffer;
	constexpr static idx_t BUFFER_LEN = 10000; // FIXME make the buffer bigger
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
};

class HTTPFileSystem : public FileSystem {
public:
	std::unique_ptr<FileHandle> OpenFile(const char *path, uint8_t flags,
	                                     FileLockType lock = FileLockType::NO_LOCK) override;

	std::vector<std::string> Glob(const std::string &path) override {
		return {path}; // FIXME
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

	virtual unique_ptr<ResponseWrapper> Request(FileHandle &handle, string url, string method,
	                                            HeaderMap header_map = {}, idx_t file_offset = 0,
	                                            char *buffer_out = nullptr, idx_t buffer_len = 0);

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	int64_t GetFileSize(FileHandle &handle) override {
		auto &sfh = (HTTPFileHandle &)handle;
		return sfh.length;
	}

	time_t GetLastModifiedTime(FileHandle &handle) override {
		auto &sfh = (HTTPFileHandle &)handle;
		return sfh.last_modified;
	}

	bool FileExists(const string &filename) override {
		try {
			auto handle = OpenFile(filename.c_str(), FileFlags::FILE_FLAGS_READ);
			auto &sfh = (HTTPFileHandle &)handle;
			if (sfh.length == 0) {
				throw std::runtime_error("not there this file");
			}
			return true;
		} catch (...) {
			return false;
		};
	}

	static void Verify();

	bool CanSeek() override {
		return false;
	}
};

} // namespace duckdb
