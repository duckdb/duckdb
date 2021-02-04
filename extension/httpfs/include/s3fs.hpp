#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

class S3FileHandle : public FileHandle {
public:
	S3FileHandle(FileSystem &fs, std::string path)
	    : FileHandle(fs, path), length(0), buffer_available(0), buffer_idx(0), file_offset(0) {
		IntializeMetadata();
		buffer = std::unique_ptr<data_t[]>(new data_t[BUFFER_LEN]);
	}

protected:
	void Close() override {
	}

private:
	void IntializeMetadata();

public:
	idx_t length;
	time_t last_modified;

	std::unique_ptr<data_t[]> buffer;
	constexpr static idx_t BUFFER_LEN = 10000; // FIXME make the buffer bigger
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
};

class S3FileSystem : public FileSystem {
public:
	S3FileSystem(std::string region_p, std::string access_key_id_p, std::string secret_access_key_p)
	    : region(region_p), access_key_id(access_key_id_p), secret_access_key(secret_access_key_p) {
	}
	std::unique_ptr<FileHandle> OpenFile(const char *path, uint8_t flags,
	                                     FileLockType lock = FileLockType::NO_LOCK) override;

	std::vector<std::string> Glob(std::string path) override {
		return {path}; // FIXME
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		throw std::runtime_error("Read3"); // unused by parquet reader
	}

	int64_t GetFileSize(FileHandle &handle) override {
		auto &sfh = (S3FileHandle &)handle;
		return sfh.length;
	}

	time_t GetLastModifiedTime(FileHandle &handle) override {
		auto &sfh = (S3FileHandle &)handle;
		return sfh.last_modified;
	}

	static void Verify();

public:
	std::string region;
	std::string access_key_id;
	std::string secret_access_key;
};

} // namespace duckdb
