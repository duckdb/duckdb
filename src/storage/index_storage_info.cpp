#include "duckdb/storage/index_storage_info.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

BufferedIndexDataInfo BufferedIndexDataInfo::FromCollection(BufferedIndexReplay replay_type,
                                                            const ColumnDataCollection &collection) {
	BufferedIndexDataInfo info;
	info.replay_type = replay_type;
	const auto column_count = collection.ColumnCount();
	info.types = collection.Types();
	info.values.resize(column_count);
	for (auto &chunk : collection.Chunks()) {
		for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
			for (idx_t r = 0; r < chunk.size(); r++) {
				info.values[c].push_back(chunk.GetValue(c, r));
			}
		}
	}
	return info;
}

unique_ptr<ColumnDataCollection> BufferedIndexDataInfo::ToColumnDataCollection(Allocator &allocator) const {
	auto collection = make_uniq<ColumnDataCollection>(allocator, types);
	if (values.empty() || types.empty()) {
		return collection;
	}
	DataChunk chunk;
	chunk.Initialize(allocator, types);

	const idx_t row_count = values[0].size();
	for (idx_t row = 0; row < row_count; row++) {
		for (idx_t col = 0; col < types.size(); col++) {
			chunk.SetValue(col, chunk.size(), values[col][row]);
		}
		chunk.SetCardinality(chunk.size() + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(chunk);
			chunk.Reset();
		}
	}
	if (chunk.size() > 0) {
		collection->Append(chunk);
	}
	return collection;
}

void IndexStorageInfo::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<string>(100, "name", name);
	serializer.WritePropertyWithDefault<idx_t>(101, "root", root);
	serializer.WritePropertyWithDefault<vector<FixedSizeAllocatorInfo>>(102, "allocator_infos", allocator_infos);
	serializer.WritePropertyWithDefault<case_insensitive_map_t<Value>>(103, "options", options,
	                                                                   case_insensitive_map_t<Value>());
	// Serialize mapped_column_ids as vector<idx_t> (since we never use child_indexes).
	vector<idx_t> mapped_ids;
	mapped_ids.reserve(mapped_column_ids.size());
	for (auto &col_id : mapped_column_ids) {
		mapped_ids.push_back(col_id.GetPrimaryIndex());
	}
	serializer.WritePropertyWithDefault<vector<idx_t>>(104, "mapped_column_ids", mapped_ids);
	serializer.WritePropertyWithDefault<vector<BufferedIndexDataInfo>>(105, "buffered_replay_data",
	                                                                   buffered_replay_data);
}

IndexStorageInfo IndexStorageInfo::Deserialize(Deserializer &deserializer) {
	IndexStorageInfo result;
	deserializer.ReadPropertyWithDefault<string>(100, "name", result.name);
	deserializer.ReadPropertyWithDefault<idx_t>(101, "root", result.root);
	deserializer.ReadPropertyWithDefault<vector<FixedSizeAllocatorInfo>>(102, "allocator_infos",
	                                                                     result.allocator_infos);
	deserializer.ReadPropertyWithExplicitDefault<case_insensitive_map_t<Value>>(103, "options", result.options,
	                                                                            case_insensitive_map_t<Value>());
	// Deserialize mapped_column_ids as vector<idx_t> and convert to vector<StorageIndex>
	vector<idx_t> mapped_ids;
	deserializer.ReadPropertyWithDefault<vector<idx_t>>(104, "mapped_column_ids", mapped_ids);
	result.mapped_column_ids.reserve(mapped_ids.size());
	for (auto &col_id : mapped_ids) {
		result.mapped_column_ids.emplace_back(col_id);
	}
	deserializer.ReadPropertyWithDefault<vector<BufferedIndexDataInfo>>(105, "buffered_replay_data",
	                                                                    result.buffered_replay_data);
	return result;
}

} // namespace duckdb
