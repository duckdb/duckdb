//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/meta_block_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/serializer.hpp"
#include "storage/block.hpp"
#include "storage/block_manager.hpp"

namespace duckdb {
//! This struct is responsible for reading meta data from disk
class MetaBlockReader : public Deserializer {
public:
	MetaBlockReader(BlockManager &manager, block_id_t block);

	BlockManager &manager;
	unique_ptr<Block> block;
	index_t offset;
	block_id_t next_block;

public:
	//! Read content of size read_size into the buffer
	void ReadData(data_ptr_t buffer, index_t read_size) override;

private:
	void ReadNewBlock(block_id_t id);
};
} // namespace duckdb
