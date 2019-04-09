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
//! This struct is responsible for organizing the reading of the data from disk to blocks
struct MetaBlockReader {
	MetaBlockReader(unique_ptr<Block> bl) : block(move(bl)) {
		ReadNewBlock(*block);
	}
	unique_ptr<Block> block;
	char buffer[BLOCK_SIZE];
	size_t size;
	size_t pos;
	block_id_t next_block;

	void ReadNewBlock(Block &block) {
		size = block.Read(buffer);
		pos = sizeof(block_id_t);
		next_block = 0;
	}

	bool Finished() {
		// finished reading
		return pos == size && next_block == 0;
	}

	void Read(char *data, size_t data_size) {
		while (pos + data_size > size) {
			// read what we can from this block
			// move to the next block
			// read remainder there
			assert(0);
		}
		// we can just read from the stream
		memcpy(data, buffer + pos, data_size);
		pos += data_size;
	}

	template <class T> T Read() {
		T element;
		Read((char *)&element, sizeof(T));
		return element;
	}

	string ReadString() {
		uint32_t size = Read<uint32_t>();
		char buffer[size + 1];
		buffer[size + 1] = '\0';
		Read(buffer, size);
		return string(buffer, size);
	}
};
} // namespace duckdb