//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/meta_block_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/serializer.hpp"
#include "storage/block.hpp"
#include "storage/block_manager.hpp"

namespace duckdb {

//! This struct is responsible for writing metadata to disk
class MetaBlockWriter : public Serializer {
public:
	MetaBlockWriter(BlockManager &manager);
	~MetaBlockWriter();

	BlockManager &manager;
	unique_ptr<Block> block;
	uint64_t offset;

public:
	void Flush();

	void WriteData(const uint8_t *buffer, uint64_t write_size) override;
};

} // namespace duckdb
