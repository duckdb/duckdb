//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/file_buffer.hpp"

namespace duckdb {

class MetaBlockReader;

class Block : public FileBuffer {
public:
	Block(Allocator &allocator, block_id_t id);
	Block(Allocator &allocator, block_id_t id, uint32_t internal_size);
	Block(FileBuffer &source, block_id_t id);

	block_id_t id;
};

struct BlockPointer {
private:
	static constexpr uint32_t ROWID_OFFSET_MASK = (1 << 31);

public:
	//! The BlockPointer value indicating invalid
	static const BlockPointer &Invalid() {
		static const BlockPointer invalid = {(block_id_t)DConstants::INVALID_INDEX, (uint32_t)0};
		return invalid;
	}
	static BlockPointer Deserialize(MetaBlockReader &reader);
	bool operator==(const BlockPointer &other) const {
		return other.block_id == block_id && other.offset == offset;
	}
	bool IsInvalid() const {
		return *this == Invalid();
	}
	//! Regular block id
	BlockPointer(block_id_t block_id_p, uint32_t offset_p) : block_id(block_id_p), offset(offset_p) {};
	BlockPointer() {};
	block_id_t block_id;
	uint32_t offset;
};

} // namespace duckdb
