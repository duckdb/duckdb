#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

class S3FileHandle : public FileHandle {
public:
	S3FileHandle(FileSystem &fs, std::string path);

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

	std::string region;
	std::string access_key_id;
	std::string secret_access_key;
};

class S3FileSystem : public FileSystem {
public:
	S3FileSystem(DatabaseInstance &instance_p) : database_instance(instance_p) {
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
	DatabaseInstance &database_instance;
};

} // namespace duckdb
