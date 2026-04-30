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

struct PerColumnMetadataBlock {
	bool is_column_index : 1;
	idx_t index : 63;
};

class PerColumnMetadataBlocks {
public:
	//! Get block IDs for a specific column (linear scan)
	vector<idx_t> GetBlocksForColumn(idx_t col_idx) const;
	//! Add a column entry with its block IDs
	void AddColumn(idx_t col_idx, const vector<idx_t> &blocks);

	//! Iterate over all block IDs (skipping column markers)
	template <typename Func>
	void ForEachBlock(Func func) const {
		for (auto &entry : data) {
			if (!entry.is_column_index) {
				func(entry.index);
			}
		}
	}

	//! Pack into a flat vector of idx_t for serialization (high bit = tag, lower 63 bits = index)
	vector<idx_t> Serialize() const;
	//! Unpack from a flat vector of idx_t
	static PerColumnMetadataBlocks Deserialize(const vector<idx_t> &packed);

private:
	vector<PerColumnMetadataBlock> data;
};

} // namespace duckdb
