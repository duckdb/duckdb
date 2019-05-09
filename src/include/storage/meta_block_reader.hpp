//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/meta_block_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "storage/block.hpp"
#include "storage/block_manager.hpp"

namespace duckdb {
//! This struct is responsible for reading meta data from disk
class MetaBlockReader {
public:
	MetaBlockReader(BlockManager &manager, block_id_t block);

	BlockManager &manager;
	unique_ptr<Block> block;
	uint64_t offset;
	block_id_t next_block;
public:
	//! Read content of size read_size into the buffer
	void Read(char *buffer, uint64_t read_size);

	template <class T> T Read() {
		T element;
		Read((char *)&element, sizeof(T));
		return element;
	}

	string ReadString() {
		uint32_t size = Read<uint32_t>();
		char buffer[size + 1];
		buffer[size] = '\0';
		Read(buffer, size);
		return string(buffer, size);
	}
private:
	void ReadNewBlock(block_id_t id);
};
} // namespace duckdb