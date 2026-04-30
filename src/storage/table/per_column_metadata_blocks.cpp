#include "duckdb/storage/table/per_column_metadata_blocks.hpp"

namespace duckdb {

static constexpr idx_t COLUMN_INDEX_BIT = idx_t(1) << 63;

vector<idx_t> PerColumnMetadataBlocks::GetBlocksForColumn(idx_t col_idx) const {
	vector<idx_t> result;
	bool found = false;
	for (auto &entry : data) {
		if (entry.is_column_index) {
			if (found) {
				break;
			}
			if (entry.index == col_idx) {
				found = true;
			}
		} else if (found) {
			result.push_back(entry.index);
		}
	}
	return result;
}

void PerColumnMetadataBlocks::AddColumn(idx_t col_idx, const vector<idx_t> &blocks) {
	PerColumnMetadataBlock marker;
	marker.is_column_index = true;
	marker.index = col_idx;
	data.push_back(marker);
	for (auto &block_id : blocks) {
		PerColumnMetadataBlock block;
		block.is_column_index = false;
		block.index = block_id;
		data.push_back(block);
	}
}

vector<idx_t> PerColumnMetadataBlocks::Serialize() const {
	vector<idx_t> result;
	result.reserve(data.size());
	for (auto &entry : data) {
		idx_t packed = entry.index;
		if (entry.is_column_index) {
			packed |= COLUMN_INDEX_BIT;
		}
		result.push_back(packed);
	}
	return result;
}

PerColumnMetadataBlocks PerColumnMetadataBlocks::Deserialize(const vector<idx_t> &packed) {
	PerColumnMetadataBlocks result;
	result.data.reserve(packed.size());
	for (auto &val : packed) {
		PerColumnMetadataBlock entry;
		entry.is_column_index = (val & COLUMN_INDEX_BIT) != 0;
		entry.index = val & ~COLUMN_INDEX_BIT;
		result.data.push_back(entry);
	}
	return result;
}

} // namespace duckdb
