//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/serializer/buffered_file_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/serializer.hpp"
#include "common/file_system.hpp"

namespace duckdb {

#define FILE_BUFFER_SIZE 4096

class BufferedFileWriter : public Serializer {
public:
	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold
	BufferedFileWriter(const char *path, index_t maximum_size = FILE_BUFFER_SIZE);

	unique_ptr<data_t[]> data;
	index_t offset;
	unique_ptr<FileHandle> handle;
public:
	void WriteData(const_data_ptr_t buffer, uint64_t write_size) override;
};

} // namespace duckdb
