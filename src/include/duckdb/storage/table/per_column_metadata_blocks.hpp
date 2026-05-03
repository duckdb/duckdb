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

	void Serialize(Serializer &serializer) const;
	static PerColumnMetadataBlock Deserialize(Deserializer &source);
};

class PerColumnMetadataBlocks {
public:
	//! Get block IDs for a specific column (linear scan)
	vector<idx_t> GetBlocksForColumn(idx_t col_idx) const;
	//! Add a column entry with its block IDs
	void AddColumn(idx_t col_idx, const vector<idx_t> &blocks);
	//! Remove a column entry and all its block IDs (linear scan)
	void RemoveColumn(idx_t col_idx);

	//! Iterate over all block IDs (skipping column markers)
	template <typename Func>
	void ForEachBlock(Func func) const {
		for (auto &entry : data) {
			if (!entry.is_column_index) {
				func(entry.index);
			}
		}
	}

	vector<PerColumnMetadataBlock> data;
};

} // namespace duckdb
