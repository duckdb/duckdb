#pragma once

#include "httpfs.hpp"
#include "duckdb/common/file_opener.hpp"

namespace duckdb {

class S3FileSystem : public HTTPFileSystem {
public:
	S3FileSystem() {
	}

	unique_ptr<ResponseWrapper> Request(FileHandle &handle, string url, string method, HeaderMap header_map = {},
	                                    idx_t file_offset = 0, char *buffer_out = nullptr,
	                                    idx_t buffer_len = 0) override;
	static void Verify();

	bool CanHandleFile(const string &fpath) override;
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}

protected:
	std::unique_ptr<HTTPFileHandle> CreateHandle(const string &path, uint8_t flags, FileLockType lock,
	                                             FileCompressionType compression, FileOpener *opener) override;
};

} // namespace duckdb
