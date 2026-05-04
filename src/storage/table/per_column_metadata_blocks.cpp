#include "duckdb/storage/table/per_column_metadata_blocks.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

static constexpr idx_t COLUMN_INDEX_BIT = idx_t(1) << 63;

void PerColumnMetadataBlock::Serialize(Serializer &serializer) const {
	idx_t packed = index;
	if (is_column_index) {
		packed |= COLUMN_INDEX_BIT;
	}
	serializer.WriteProperty<idx_t>(100, "packed", packed);
}

PerColumnMetadataBlock PerColumnMetadataBlock::Deserialize(Deserializer &deserializer) {
	auto packed = deserializer.ReadProperty<idx_t>(100, "packed");
	PerColumnMetadataBlock result;
	result.is_column_index = (packed & COLUMN_INDEX_BIT) != 0;
	result.index = packed & ~COLUMN_INDEX_BIT;
	return result;
}

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
