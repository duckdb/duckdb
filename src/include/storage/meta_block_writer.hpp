//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/meta_block_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "storage/block.hpp"
#include "storage/block_manager.hpp"

namespace duckdb {

//! This struct is responsible for organizing the writing of the data within a block
class MetaBlockWriter {
public:
	MetaBlockWriter(BlockManager &manager);
	~MetaBlockWriter();

	BlockManager &manager;
	unique_ptr<Block> block;
	uint64_t offset;
public:
	void Flush();

	void Write(const char *data, uint64_t data_size);

	template <class T> void Write(T element) {
		Write((const char *)&element, sizeof(T));
	}

	void WriteString(string &str) {
		Write<uint32_t>(str.size());
		Write(str.c_str(), str.size());
	}
};

} // namespace duckdb