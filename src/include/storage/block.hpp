//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/block.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/fstream_util.hpp"

namespace duckdb {
class DataChunk;
// Size of a memory slot managed by the StorageManager. This is the quantum of allocation for Blocks on DuckDB. 1 MB is
// the large page size on x86.
constexpr const size_t BLOCK_SIZE = 0x100000;
// The storage works as a tree where the root node should be fetched easily. Therefore the root size is 4k which is the
// basic hardware page size
constexpr const size_t ROOT_BLOCK_SIZE = 0x1000;

using block_id_t = size_t;
using version_t = size_t;

//! Block is an abstract representation for memory chunks on duckdb, it may have different concrete implementations.
class Block {
public:
	Block(block_id_t id) : id(id) {
	}
	virtual ~Block() {
	}

	//! Writes new contents to the block
	virtual void Write(char *buffer, size_t count) = 0;
	//! Read the entire contents of the block into a buffer
	virtual size_t Read(char *buffer) = 0;
	//! Read a set amount of bytes from a block into a buffer
	virtual void Read(char *buffer, size_t offset, size_t count) = 0;

	virtual bool HasNoSpace(DataChunk &chunk) = 0;

	template <class T> T Read(uint32_t offset) {
		T element;
		Read((char *)&element, offset, sizeof(T));
		return element;
	}
	string ReadString(uint32_t offset) {
		uint32_t size = Read<uint32_t>(offset);
		char buffer[size + 1];
		buffer[size + 1] = '\0';
		Read(buffer, offset, size);
		return string(buffer, size);
	}

	block_id_t id{0};
	size_t offset{0};
	size_t tuple_count{0};
};
} // namespace duckdb