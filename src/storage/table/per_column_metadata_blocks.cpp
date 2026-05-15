#include "duckdb/storage/table/per_column_metadata_blocks.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

static constexpr idx_t COLUMN_INDEX_BIT = idx_t(1) << 63;

idx_t PerColumnMetadataBlock::GetPacked() {
	idx_t packed = index;
	if (is_column_index) {
		packed |= COLUMN_INDEX_BIT;
	}
	return packed;
}

PerColumnMetadataBlock PerColumnMetadataBlock::Unpack(idx_t packed) {
	PerColumnMetadataBlock result;
	result.is_column_index = (packed & COLUMN_INDEX_BIT) != 0;
	result.index = packed & ~COLUMN_INDEX_BIT;
	return result;
}

vector<vector<idx_t>> PerColumnMetadataBlocks::GetBlocksForColumns(const vector<idx_t> &columns) const {
	vector<vector<idx_t>> result(columns.size());
	if (columns.empty()) {
		return result;
	}
	idx_t col_pos = 0;
	bool collecting = false;
	for (auto &entry : data) {
		if (entry.is_column_index) {
			collecting = false;
			// skip past requested columns that are before the current entry
			while (col_pos < columns.size() && columns[col_pos] < entry.index) {
				col_pos++;
			}
			if (col_pos >= columns.size()) {
				break;
			}
			if (columns[col_pos] == entry.index) {
				collecting = true;
			}
		} else if (collecting) {
			result[col_pos].push_back(entry.index);
		}
	}
	return result;
}

void PerColumnMetadataBlocks::AddColumn(idx_t col_idx, const vector<idx_t> &blocks) {
	if (blocks.empty()) {
		return;
	}
#ifdef D_ASSERT_IS_ENABLED
	// assert sorted insertion: col_idx must be greater than the last column index
	for (idx_t i = data.size(); i > 0; i--) {
		if (data[i - 1].is_column_index) {
			D_ASSERT(col_idx > data[i - 1].index);
			break;
		}
	}
#endif
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

void PerColumnMetadataBlocks::RemoveColumn(idx_t col_idx) {
	idx_t start = data.size();
	idx_t end = data.size();
	for (idx_t i = 0; i < data.size(); i++) {
		if (data[i].is_column_index && data[i].index == col_idx) {
			start = i;
			// find the end: next column marker or end of data
			end = data.size();
			for (idx_t j = i + 1; j < data.size(); j++) {
				if (data[j].is_column_index) {
					end = j;
					break;
				}
			}
			break;
		}
	}
	if (start < data.size()) {
		data.erase(data.begin() + NumericCast<int64_t>(start), data.begin() + NumericCast<int64_t>(end));
	}
}

} // namespace duckdb
