//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/directory_block.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "storage/block.hpp"

namespace duckdb {

//! DirectoryBlock implements a Block organized as multiple files, one per block.
class DirectoryBlock : public Block {
public:
	//! Constructor of a DirectoryBlock. The inputs are the block_id and the path to the file
	DirectoryBlock(block_id_t id, const string &path) : Block(id), path(path) {
	}
	//! Writes the buffer to the referenced file
	void Write(char *buffer, size_t count) override;
	//! Reads the whole block(file) and loads it to the given buffer
	size_t Read(char *buffer) override;
	//! Reads the determined chunk of data(length) starting from the offset and loads it to the buffer
	void Read(char *buffer, size_t offset, size_t buffered_size) override;

private:
	const string path;
};
} // namespace duckdb