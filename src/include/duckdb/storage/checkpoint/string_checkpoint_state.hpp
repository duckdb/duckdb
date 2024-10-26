//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/string_checkpoint_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {
struct UncompressedStringSegmentState;

class OverflowStringWriter {
public:
	virtual ~OverflowStringWriter() {
	}

	virtual void WriteString(UncompressedStringSegmentState &state, string_t string, block_id_t &result_block,
	                         int32_t &result_offset) = 0;
	virtual void Flush() = 0;
};

struct StringBlock {
	shared_ptr<BlockHandle> block;
	idx_t offset;
	idx_t size;
	unique_ptr<StringBlock> next;
};

struct string_location_t { // NOLINT
	string_location_t(block_id_t block_id, int32_t offset) : block_id(block_id), offset(offset) {
	}
	string_location_t() {
	}
	bool IsValid(const idx_t block_size) {
		auto cast_block_size = NumericCast<int32_t>(block_size);
		return offset < cast_block_size && (block_id == INVALID_BLOCK || block_id >= MAXIMUM_BLOCK);
	}
	block_id_t block_id;
	int32_t offset;
};

struct UncompressedStringSegmentState : public CompressedSegmentState {
	~UncompressedStringSegmentState() override;

	//! The string block holding strings that do not fit in the main block
	//! FIXME: this should be replaced by a heap that also allows freeing of unused strings
	unique_ptr<StringBlock> head;
	//! Map of block id to string block
	unordered_map<block_id_t, reference<StringBlock>> overflow_blocks;
	//! Overflow string writer (if any), if not set overflow strings will be written to memory blocks
	unique_ptr<OverflowStringWriter> overflow_writer;
	//! The set of overflow blocks written to disk (if any)
	vector<block_id_t> on_disk_blocks;

public:
	shared_ptr<BlockHandle> GetHandle(BlockManager &manager, block_id_t block_id);

	void RegisterBlock(BlockManager &manager, block_id_t block_id);

	string GetSegmentInfo() const override {
		if (on_disk_blocks.empty()) {
			return "";
		}
		string result = StringUtil::Join(on_disk_blocks, on_disk_blocks.size(), ", ",
		                                 [&](block_id_t block) { return to_string(block); });
		return "Overflow String Block Ids: " + result;
	}

private:
	mutex block_lock;
	unordered_map<block_id_t, shared_ptr<BlockHandle>> handles;
};

} // namespace duckdb
