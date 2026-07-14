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
	//! Clear a column's entry and all its block IDs in place, leaving the indices of the other
	//! columns untouched (linear scan), for use when the column is rewritten (e.g. ALTER TYPE)
	void ClearColumn(idx_t col_idx);
	//! Remove a column's entry and shift down the indices of all subsequent columns,
	//! for use when the column is positionally removed (e.g. DROP COLUMN)
	void RemoveColumn(idx_t col_idx);
	//! Merge two PerColumnMetadataBlocks sorted by column index with disjoint column sets
	static PerColumnMetadataBlocks Merge(const PerColumnMetadataBlocks &a, const PerColumnMetadataBlocks &b);

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
