#pragma once

#include "httpfs.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

class S3FileSystem : public HTTPFileSystem {
public:
	S3FileSystem(DatabaseInstance &instance_p) : database_instance(instance_p) {
	}
	std::unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = DEFAULT_LOCK,
	                                     FileCompressionType compression = DEFAULT_COMPRESSION,
	                                     FileOpener *opener = nullptr) override;

	unique_ptr<ResponseWrapper> Request(FileHandle &handle, string url, string method, HeaderMap header_map = {},
	                                    idx_t file_offset = 0, char *buffer_out = nullptr,
	                                    idx_t buffer_len = 0) override;
	static void Verify();

	bool CanHandleFile(const string &fpath) override;
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}

public:
	DatabaseInstance &database_instance;

private:
	HeaderMap CreateAuthHeaders(string host, string path, string method);
};

} // namespace duckdb
