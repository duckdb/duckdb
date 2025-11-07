#include "duckdb/execution/index/unbound_index.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

BufferedIndexData::BufferedIndexData(BufferedIndexReplay replay_type, unique_ptr<ColumnDataCollection> data_p)
    : type(replay_type), data(std::move(data_p)) {
}

UnboundIndex::UnboundIndex(unique_ptr<CreateInfo> create_info, IndexStorageInfo storage_info_p,
                           TableIOManager &table_io_manager, AttachedDatabase &db)
    : Index(create_info->Cast<CreateIndexInfo>().column_ids, table_io_manager, db), create_info(std::move(create_info)),
      storage_info(std::move(storage_info_p)) {
	// Memory safety check.
	for (idx_t info_idx = 0; info_idx < storage_info.allocator_infos.size(); info_idx++) {
		auto &info = storage_info.allocator_infos[info_idx];
		for (idx_t buffer_idx = 0; buffer_idx < info.buffer_ids.size(); buffer_idx++) {
			if (info.buffer_ids[buffer_idx] > idx_t(MAX_ROW_ID)) {
				throw InternalException("found invalid buffer ID in UnboundIndex constructor");
			}
		}
	}
}

void UnboundIndex::CommitDrop() {
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	for (auto &info : storage_info.allocator_infos) {
		for (auto &block : info.block_pointers) {
			if (block.IsValid()) {
				block_manager.MarkBlockAsModified(block.block_id);
			}
		}
	}
}

void UnboundIndex::BufferChunk(DataChunk &index_column_chunk, Vector &row_ids,
                               const vector<StorageIndex> &mapped_column_ids_p, BufferedIndexReplay replay_type) {
	D_ASSERT(!column_ids.empty());
	auto types = index_column_chunk.GetTypes(); // column types
	types.push_back(LogicalType::ROW_TYPE);

	auto &allocator = Allocator::Get(db);

	BufferedIndexData buffered_data(replay_type, make_uniq<ColumnDataCollection>(allocator, types));

	//! First time we are buffering data, canonical column_id mapping is stored.
	//! This should be a sorted list of all the physical offsets of Indexed columns on this table.
	if (mapped_column_ids.empty()) {
		mapped_column_ids = mapped_column_ids_p;
	}
	D_ASSERT(mapped_column_ids == mapped_column_ids_p);

	// Combined chunk has all the indexed columns and rowids.
	DataChunk combined_chunk;
	combined_chunk.InitializeEmpty(types);
	for (idx_t i = 0; i < index_column_chunk.ColumnCount(); i++) {
		combined_chunk.data[i].Reference(index_column_chunk.data[i]);
	}
	combined_chunk.data.back().Reference(row_ids);
	combined_chunk.SetCardinality(index_column_chunk.size());
	buffered_data.data->Append(combined_chunk);
	buffered_replays.emplace_back(std::move(buffered_data));
}

} // namespace duckdb
