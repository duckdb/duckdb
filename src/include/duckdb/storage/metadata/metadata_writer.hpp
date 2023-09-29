//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/metadata/metadata_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/common/serializer/write_stream.hpp"

namespace duckdb {

class MetadataWriter : public WriteStream {
public:
	explicit MetadataWriter(MetadataManager &manager,
	                        optional_ptr<vector<MetaBlockPointer>> written_pointers = nullptr);
	MetadataWriter(const MetadataWriter &) = delete;
	MetadataWriter &operator=(const MetadataWriter &) = delete;
	~MetadataWriter() override;

public:
	void WriteData(const_data_ptr_t buffer, idx_t write_size) override;
	void Flush();

	BlockPointer GetBlockPointer();
	MetaBlockPointer GetMetaBlockPointer();
	MetadataManager &GetManager() {
		return manager;
	}

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
	optional_ptr<vector<MetaBlockPointer>> written_pointers;
	idx_t capacity;
	idx_t offset;
};

} // namespace duckdb
