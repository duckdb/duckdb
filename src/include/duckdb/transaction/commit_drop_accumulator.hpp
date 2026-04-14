//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/commit_drop_accumulator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {

class BlockManager;
class FixedSizeBuffer;

struct CommitDropAccumulator {
	struct BlockMark {
		reference<BlockManager> bm;
		block_id_t id;
	};
	struct BufferMark {
		reference<BlockManager> bm;
		reference<FixedSizeBuffer> buffer;
		block_id_t id;
	};

	vector<BlockMark> block_marks;
	vector<BufferMark> buffer_marks;

	void AddBlock(BlockManager &bm, block_id_t id) {
		block_marks.push_back(BlockMark {bm, id});
	}
	void AddBuffer(BlockManager &bm, FixedSizeBuffer &buffer, block_id_t id) {
		buffer_marks.push_back(BufferMark {bm, buffer, id});
	}

	void Apply();
	void Clear() {
		block_marks.clear();
		buffer_marks.clear();
	}
	bool Empty() const {
		return block_marks.empty() && buffer_marks.empty();
	}
};

} // namespace duckdb
