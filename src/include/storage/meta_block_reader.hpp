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
	uint64_t offset;
	block_id_t next_block;

public:
	//! Read content of size read_size into the buffer
	void ReadData(uint8_t *buffer, uint64_t read_size) override;

private:
	void ReadNewBlock(block_id_t id);
};
} // namespace duckdb
