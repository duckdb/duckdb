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

	void ReadData(Context &context, data_ptr_t buffer, idx_t read_size) override;

	MetaBlockPointer GetMetaBlockPointer();

	MetadataManager &GetMetadataManager() {
		return manager;
	}

private:
	data_ptr_t BasePtr();
	data_ptr_t Ptr();

	void ReadNextBlock();

	void ReadNextBlock(Context &context);

	MetadataPointer FromDiskPointer(MetaBlockPointer pointer);

	//! Throws if the chain of metadata blocks loops back to a block that was already read
	void CheckForCycle();

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
	//! Cycle detection: the pointer of the block that was read when blocks_read reached the previous power of two
	idx_t cycle_check_pointer;
	idx_t cycle_check_at;
	idx_t blocks_read;
};

} // namespace duckdb
