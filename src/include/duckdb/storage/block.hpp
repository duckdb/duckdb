//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

class Block : public FileBuffer {
public:
	Block(Allocator &allocator, const block_id_t id, const idx_t block_size);
	Block(Allocator &allocator, block_id_t id, uint32_t internal_size);
	Block(FileBuffer &source, block_id_t id);

	block_id_t id;
};

struct BlockPointer {
	BlockPointer(block_id_t block_id_p, uint32_t offset_p) : block_id(block_id_p), offset(offset_p) {
	}
	BlockPointer() : block_id(INVALID_BLOCK), offset(0) {
	}

	block_id_t block_id;
	uint32_t offset;

	bool IsValid() const {
		return block_id != INVALID_BLOCK;
	}

	void Serialize(Serializer &serializer) const;
	static BlockPointer Deserialize(Deserializer &source);
};

struct MetaBlockPointer {
	MetaBlockPointer(idx_t block_pointer, uint32_t offset_p) : block_pointer(block_pointer), offset(offset_p) {
	}
	MetaBlockPointer() : block_pointer(DConstants::INVALID_INDEX), offset(0) {
	}

	idx_t block_pointer;
	uint32_t offset;

	bool IsValid() const {
		return block_pointer != DConstants::INVALID_INDEX;
	}
	block_id_t GetBlockId() const;
	uint32_t GetBlockIndex() const;

	void Serialize(Serializer &serializer) const;
	static MetaBlockPointer Deserialize(Deserializer &source);
};

} // namespace duckdb
