//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/per_column_metadata_blocks.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

struct PerColumnMetadataBlock {
	bool is_column_index : 1;
	idx_t index : 63;

	idx_t GetPacked();

	static PerColumnMetadataBlock Unpack(idx_t packed);
};

class PerColumnMetadataBlocks {
public:
	//! Get block IDs for specific columns (linear scan), returns one vector per requested column
	vector<vector<idx_t>> GetBlocksForColumns(const vector<idx_t> &columns) const;

	//! Add a column entry with its block IDs
	void AddColumn(idx_t col_idx, const vector<idx_t> &blocks);
	//! Remove a column entry and all its block IDs (linear scan)
	void RemoveColumn(idx_t col_idx);

	//! Iterate over all block IDs, passing (column_index, block_id) to the callback
	template <typename Func>
	void ForEachBlock(Func func) const {
		idx_t current_col = 0;
		for (auto &entry : data) {
			if (entry.is_column_index) {
				current_col = entry.index;
			} else {
				func(current_col, entry.index);
			}
		}
	}

	vector<PerColumnMetadataBlock> data;
};

} // namespace duckdb
