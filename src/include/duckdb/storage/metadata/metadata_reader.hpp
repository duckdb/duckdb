//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/metadata/metadata_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "metadata_manager.hpp"

namespace duckdb {

class MetadataReader : public Deserializer {
public:
	MetadataReader(MetadataManager &manager, MetadataPointer next_pointer);

public:
	//! Read content of size read_size into the buffer
	void ReadData(data_ptr_t buffer, idx_t read_size) override;

private:
	data_ptr_t Ptr();

	void ReadNextBlock();

private:
	MetadataManager &manager;
	MetadataHandle block;
	MetadataPointer next_pointer;
	bool has_next_block;
	idx_t offset;
	idx_t capacity;
};

} // namespace duckdb
