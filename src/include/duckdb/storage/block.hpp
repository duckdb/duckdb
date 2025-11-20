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

class BlockAllocator;
class Serializer;
class Deserializer;

class Block : public FileBuffer {
public:
	Block(BlockAllocator &allocator, const block_id_t id, const idx_t block_size, const idx_t block_header_size);
	Block(BlockAllocator &allocator, block_id_t id, uint32_t internal_size, idx_t block_header_size);
	Block(BlockAllocator &allocator, const block_id_t id, BlockManager &block_manager);
	Block(FileBuffer &source, block_id_t id, idx_t block_header_size);

	block_id_t id;
};

struct BlockPointer {
	BlockPointer(block_id_t block_id_p, uint32_t offset_p) : block_id(block_id_p), offset(offset_p) {
	}
	BlockPointer() : block_id(INVALID_BLOCK), offset(0) {
	}

	block_id_t block_id;
	uint32_t offset;
	uint32_t unused_padding {0};

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
	uint32_t unused_padding {0};

	bool IsValid() const {
		return block_pointer != DConstants::INVALID_INDEX;
	}
	block_id_t GetBlockId() const;
	uint32_t GetBlockIndex() const;

	bool operator==(const MetaBlockPointer &rhs) const {
		return block_pointer == rhs.block_pointer && offset == rhs.offset;
	}

	friend std::ostream &operator<<(std::ostream &os, const MetaBlockPointer &obj) {
		return os << "{block_id: " << obj.GetBlockId() << " index: " << obj.GetBlockIndex() << " offset: " << obj.offset
		          << "}";
	}

	void Serialize(Serializer &serializer) const;
	static MetaBlockPointer Deserialize(Deserializer &source);
};

} // namespace duckdb
