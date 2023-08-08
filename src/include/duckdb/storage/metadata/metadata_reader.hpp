//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/metadata/metadata_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/metadata/metadata_manager.hpp"

namespace duckdb {

enum class BlockReaderType { EXISTING_BLOCKS, REGISTER_BLOCKS };

class MetadataReader : public Deserializer {
public:
	MetadataReader(MetadataManager &manager, MetaBlockPointer pointer,
	               BlockReaderType type = BlockReaderType::EXISTING_BLOCKS);
	MetadataReader(MetadataManager &manager, BlockPointer pointer);
	~MetadataReader() override;

public:
	//! Read content of size read_size into the buffer
	void ReadData(data_ptr_t buffer, idx_t read_size) override;

	MetaBlockPointer GetMetaBlockPointer();

	MetadataManager &GetMetadataManager() {
		return manager;
	}

private:
	data_ptr_t BasePtr();
	data_ptr_t Ptr();

	void ReadNextBlock();

	MetadataPointer FromDiskPointer(MetaBlockPointer pointer);

private:
	MetadataManager &manager;
	BlockReaderType type;
	MetadataHandle block;
	MetadataPointer next_pointer;
	bool has_next_block;
	idx_t index;
	idx_t offset;
	idx_t next_offset;
	idx_t capacity;
};

} // namespace duckdb
