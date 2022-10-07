//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/meta_block_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {
class BlockHandle;
class BlockManager;
class BufferHandle;
class DatabaseInstance;

//! This struct is responsible for reading meta data from disk
class MetaBlockReader : public Deserializer {
public:
	MetaBlockReader(BlockManager &block_manager, block_id_t block, bool free_blocks_on_read = true);
	~MetaBlockReader() override;

	BlockManager &block_manager;
	shared_ptr<BlockHandle> block;
	BufferHandle handle;
	idx_t offset;
	block_id_t next_block;
	bool free_blocks_on_read;

public:
	//! Read content of size read_size into the buffer
	void ReadData(data_ptr_t buffer, idx_t read_size) override;

private:
	void ReadNewBlock(block_id_t id);
};
} // namespace duckdb
