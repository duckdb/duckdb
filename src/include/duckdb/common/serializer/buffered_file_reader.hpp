//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/buffered_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/buffered_file_writer.hpp"

namespace duckdb {

class BufferedFileReader : public Deserializer {
public:
	BufferedFileReader(FileSystem &fs, const char *path, optional_ptr<ClientContext> context,
	                   FileLockType lock_type = FileLockType::READ_LOCK, optional_ptr<FileOpener> opener = nullptr);

	FileSystem &fs;
	unsafe_unique_array<data_t> data;
	idx_t offset;
	idx_t read_data;
	unique_ptr<FileHandle> handle;

public:
	void ReadData(data_ptr_t buffer, uint64_t read_size) override;
	//! Returns true if the reader has finished reading the entire file
	bool Finished();

	idx_t FileSize() {
		return file_size;
	}

	void Seek(uint64_t location);
	uint64_t CurrentOffset();

	ClientContext &GetContext() override;

	optional_ptr<Catalog> GetCatalog() override;
	void SetCatalog(Catalog &catalog);

private:
	idx_t file_size;
	idx_t total_read;
	optional_ptr<ClientContext> context;
	optional_ptr<Catalog> catalog;
};

} // namespace duckdb
