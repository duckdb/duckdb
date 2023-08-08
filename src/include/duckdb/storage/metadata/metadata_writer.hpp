//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/metadata/metadata_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/metadata/metadata_manager.hpp"

namespace duckdb {

class MetadataWriter : public Serializer {
public:
	explicit MetadataWriter(MetadataManager &manager);
	~MetadataWriter() override;

public:
	void WriteData(const_data_ptr_t buffer, idx_t write_size) override;

	void Flush();

	BlockPointer GetBlockPointer();
	MetaBlockPointer GetMetaBlockPointer();

protected:
	virtual MetadataHandle NextHandle();

private:
	data_ptr_t BasePtr();
	data_ptr_t Ptr();

	void NextBlock();

private:
	MetadataManager &manager;
	MetadataHandle block;
	MetadataPointer current_pointer;
	idx_t capacity;
	idx_t offset;
};

} // namespace duckdb
