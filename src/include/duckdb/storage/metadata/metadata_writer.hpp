//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/metadata/metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "metadata_manager.hpp"

namespace duckdb {

class MetadataWriter : public Serializer {
public:
	MetadataWriter(MetadataManager &manager);

public:
	void WriteData(const_data_ptr_t buffer, idx_t write_size) override;

private:
	data_ptr_t Ptr();

	void NextBlock();

private:
	MetadataManager &manager;
	MetadataHandle block;
	MetadataPointer current_pointer;
	vector<MetadataPointer> written_blocks;
	idx_t capacity;
	idx_t offset;
};

} // namespace duckdb
