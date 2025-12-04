//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/index_storage_info.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

void IndexStorageInfo::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<string>(100, "name", name);
	serializer.WritePropertyWithDefault<idx_t>(101, "root", root);
	serializer.WritePropertyWithDefault<vector<FixedSizeAllocatorInfo>>(102, "allocator_infos", allocator_infos);
	serializer.WritePropertyWithDefault<case_insensitive_map_t<Value>>(103, "options", options,
	                                                                   case_insensitive_map_t<Value>());

	// const bool has_buffered_replays = buffered_replays && !buffered_replays->empty();
	// serializer.WritePropertyWithDefault<bool>(104, "has_buffered_replays", has_buffered_replays, false);
	// if (has_buffered_replays) {
	// 	serializer.WriteList(105, "buffered_replays", buffered_replays->size(), [&](Serializer::List &list, idx_t idx) {
	// 		list.WriteObject([&](Serializer &object) { (*buffered_replays)[idx].Serialize(object); });
	// 	});
	// }
	// // Convert StorageIndex to idx_t for serialization
	// vector<idx_t> mapped_ids;
	// mapped_ids.reserve(mapped_column_ids.size());
	// for (const auto &storage_idx : mapped_column_ids) {
	// 	mapped_ids.push_back(storage_idx.GetPrimaryIndex());
	// }
	// serializer.WritePropertyWithDefault<vector<idx_t>>(106, "mapped_column_ids", mapped_ids);
}

IndexStorageInfo IndexStorageInfo::Deserialize(Deserializer &deserializer) {
	IndexStorageInfo result;
	deserializer.ReadPropertyWithDefault<string>(100, "name", result.name);
	deserializer.ReadPropertyWithDefault<idx_t>(101, "root", result.root);
	deserializer.ReadPropertyWithDefault<vector<FixedSizeAllocatorInfo>>(102, "allocator_infos",
	                                                                     result.allocator_infos);
	deserializer.ReadPropertyWithExplicitDefault<case_insensitive_map_t<Value>>(103, "options", result.options,
	                                                                            case_insensitive_map_t<Value>());
	// auto has_buffered_replays = deserializer.ReadPropertyWithExplicitDefault<bool>(104, "has_buffered_replays",
	// false); if (has_buffered_replays) { 	auto buffered_data = make_uniq<vector<BufferedIndexData>>();
	// 	deserializer.ReadList(105, "buffered_replays", [&](Deserializer::List &list, idx_t) {
	// 		list.ReadObject(
	// 		    [&](Deserializer &object) { buffered_data->push_back(BufferedIndexData::Deserialize(object)); });
	// 	});
	// 	result.buffered_replays = std::move(buffered_data);
	// }
	// vector<idx_t> mapped_ids;
	// deserializer.ReadPropertyWithDefault<vector<idx_t>>(106, "mapped_column_ids", mapped_ids);
	// // Convert idx_t back to StorageIndex
	// result.mapped_column_ids.reserve(mapped_ids.size());
	// for (const auto &idx : mapped_ids) {
	// 	result.mapped_column_ids.emplace_back(idx);
	// }
	return result;
}

} // namespace duckdb
