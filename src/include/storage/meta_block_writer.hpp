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
struct MetaBlockWriter {
	MetaBlockWriter(BlockManager &manager) : manager(manager) {
		current_block = manager.CreateBlock();
		buffered_size = sizeof(block_id_t);
	}
	~MetaBlockWriter() {
		Flush();
	}

	BlockManager &manager;
	char buffer[BLOCK_SIZE];
	size_t buffered_size;
	unique_ptr<Block> current_block;

	void Flush() {
		if (buffered_size > 0) {
			current_block->Write(buffer, buffered_size);
		}
	}

	void Write(const char *data, size_t data_size) {
		while (buffered_size + data_size >= BLOCK_SIZE) {
			// we need to make a new block
			// first copy what we can
			size_t copy_amount = BLOCK_SIZE - buffered_size;
			assert(copy_amount < data_size);
			memcpy(buffer + buffered_size, data, copy_amount);
			data += copy_amount;
			data_size -= copy_amount;
			// now we need to get a new block
			auto new_block = manager.CreateBlock();
			memcpy(buffer, &new_block->id, sizeof(block_id_t));
			// first flush the old block
			Flush();
			// then replace the old block by the new block
			current_block = move(new_block);
			buffered_size = sizeof(block_id_t);
		}
		memcpy(buffer + buffered_size, data, data_size);
		buffered_size += data_size;
	}

	template <class T> void Write(T element) {
		Write((const char *)&element, sizeof(T));
	}

	void WriteString(string &str) {
		Write<uint32_t>(str.size());
		Write(str.c_str(), str.size());
	}
};

} // namespace duckdb