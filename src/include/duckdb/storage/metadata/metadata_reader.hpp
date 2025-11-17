//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/metadata/metadata_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/common/serializer/read_stream.hpp"

namespace duckdb {

enum class BlockReaderType { EXISTING_BLOCKS, REGISTER_BLOCKS };

class MetadataReader : public ReadStream {
public:
	MetadataReader(MetadataManager &manager, MetaBlockPointer pointer,
	               optional_ptr<vector<MetaBlockPointer>> read_pointers = nullptr,
	               BlockReaderType type = BlockReaderType::EXISTING_BLOCKS);
	MetadataReader(MetadataManager &manager, BlockPointer pointer);
	~MetadataReader() override;

public:
	//! Read content of size read_size into the buffer
	void ReadData(data_ptr_t buffer, idx_t read_size) override;

	void ReadData(QueryContext context, data_ptr_t buffer, idx_t read_size) override;

	MetaBlockPointer GetMetaBlockPointer();

	MetadataManager &GetMetadataManager() {
		return manager;
	}
	//! Gets a list of all remaining blocks to be read by this metadata reader - consumes all blocks
	//! If "last_block" is specified, we stop when reaching that block
	vector<MetaBlockPointer> GetRemainingBlocks(MetaBlockPointer last_block = MetaBlockPointer());

private:
	data_ptr_t BasePtr();
	data_ptr_t Ptr();

	void ReadNextBlock();

	void ReadNextBlock(QueryContext context);

	MetadataPointer FromDiskPointer(MetaBlockPointer pointer);

private:
	MetadataManager &manager;
	BlockReaderType type;
	MetadataHandle block;
	MetaBlockPointer next_pointer;
	bool has_next_block;
	optional_ptr<vector<MetaBlockPointer>> read_pointers;
	idx_t index;
	idx_t offset;
	idx_t next_offset;
	idx_t capacity;
};

} // namespace duckdb
