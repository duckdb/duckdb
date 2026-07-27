#include "duckdb/execution/index/unbound_index.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

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

	// Create the stable per-index layout used by buffered replay chunks.
	mapped_column_ids.reserve(column_id_set.size());
	for (auto &col : column_id_set) {
		mapped_column_ids.emplace_back(col);
	}
	std::sort(mapped_column_ids.begin(), mapped_column_ids.end());
}

void UnboundIndex::ResetStorage() {
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	for (auto &info : storage_info.allocator_infos) {
		for (auto &block : info.block_pointers) {
			if (block.IsValid()) {
				block_manager.MarkBlockAsModified(block.block_id);
			}
		}
	}
}

void UnboundIndex::BufferChunk(DataChunk &table_chunk, Vector &row_ids, const BufferedIndexReplay replay_type) {
	D_ASSERT(!column_ids.empty());

	// table_chunk is in physical table layout: data[j] holds the data of physical column j.
	// Reference this index's own columns directly by their physical offset.
	vector<LogicalType> types;
	types.reserve(mapped_column_ids.size() + 1);
	for (auto &col : mapped_column_ids) {
		if (col.GetPrimaryIndex() >= table_chunk.ColumnCount()) {
			throw InternalException("UnboundIndex::BufferChunk: indexed column %llu of index %s is out of range for "
			                        "the table chunk",
			                        col.GetPrimaryIndex(), GetIndexName());
		}
		types.push_back(table_chunk.data[col.GetPrimaryIndex()].GetType());
	}
	types.push_back(LogicalType::ROW_TYPE);

	// combined_chunk has this index's columns in mapped_column_ids ordering, as well as a rowid column.
	DataChunk combined_chunk;
	combined_chunk.InitializeEmpty(types);
	for (idx_t i = 0; i < mapped_column_ids.size(); i++) {
		combined_chunk.data[i].Reference(table_chunk.data[mapped_column_ids[i].GetPrimaryIndex()]);
	}
	combined_chunk.data.back().Reference(row_ids);

	auto &allocator = Allocator::Get(db);
	auto &buffer = buffered_replays.GetBuffer(replay_type);
	if (buffer == nullptr) {
		buffer = make_uniq<ColumnDataCollection>(allocator, types);
	}
	// The starting index of the buffer range is the size of the buffer.
	const idx_t start = buffer->Count();
	const idx_t end = start + combined_chunk.size();
	auto &ranges = buffered_replays.ranges;

	if (ranges.empty() || ranges.back().type != replay_type) {
		// If there are no buffered ranges, or the replay types don't match, append a new range.
		ranges.emplace_back(replay_type, start, end);
		buffer->Append(combined_chunk);
		return;
	}
	// Otherwise merge the range with the previous one.
	ranges.back().end = end;
	buffer->Append(combined_chunk);
}

} // namespace duckdb
