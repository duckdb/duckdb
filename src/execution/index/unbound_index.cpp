#include "duckdb/execution/index/unbound_index.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

BufferedIndexData::BufferedIndexData(BufferedIndexReplay replay_type)
    : type(replay_type), data(nullptr), small_chunk(nullptr) {
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

void UnboundIndex::ResizeSmallChunk(unique_ptr<DataChunk> &small_chunk, Allocator &allocator,
                                    const vector<LogicalType> &types) {
	if (!small_chunk) {
		return;
	}
	if (small_chunk->size() == 0) {
		small_chunk.reset();
		return;
	}
	// Only resize if unused space exceeds RESIZE_THRESHOLD.
	auto unused_space = small_chunk->GetCapacity() - small_chunk->size();
	if (unused_space < RESIZE_THRESHOLD) {
		return;
	}
	const auto current_size = small_chunk->size();
	auto old_chunk = std::move(small_chunk);
	small_chunk = make_uniq<DataChunk>();
	small_chunk->Initialize(allocator, types, current_size);
	small_chunk->Append(*old_chunk, false);
}

void UnboundIndex::BufferChunk(DataChunk &index_column_chunk, Vector &row_ids,
                               const vector<StorageIndex> &mapped_column_ids_p, BufferedIndexReplay replay_type) {
	D_ASSERT(!column_ids.empty());
	auto types = index_column_chunk.GetTypes(); // column types
	types.push_back(LogicalType::ROW_TYPE);

	auto &allocator = Allocator::Get(db);

	//! First time we are buffering data, canonical column_id mapping is stored.
	//! This should be a sorted list of all the physical offsets of Indexed columns on this table.
	if (mapped_column_ids.empty()) {
		mapped_column_ids = mapped_column_ids_p;
	}
	D_ASSERT(mapped_column_ids == mapped_column_ids_p);

	// combined_chunk has all the indexed columns according to mapped_column_ids ordering, as well as a rowid column.
	DataChunk combined_chunk;
	combined_chunk.InitializeEmpty(types);
	for (idx_t i = 0; i < index_column_chunk.ColumnCount(); i++) {
		combined_chunk.data[i].Reference(index_column_chunk.data[i]);
	}
	combined_chunk.data.back().Reference(row_ids);
	combined_chunk.SetCardinality(index_column_chunk.size());

	// If the replay type doesn't match the type of the BufferedIndexData at the end, we append a new
	// BufferedIndexData that we can buffer into.
	if (buffered_replays.empty() || buffered_replays.back().type != replay_type) {
		// Before creating a new BufferedIndexData, resize the previous small_chunk if it has too many free slots,
		// or destroy it if it's empty.
		if (!buffered_replays.empty()) {
			auto &prev = buffered_replays.back();
			ResizeSmallChunk(prev.small_chunk, allocator, types);
		}
		BufferedIndexData buffered_data(replay_type);
		buffered_replays.emplace_back(std::move(buffered_data));
	}
	// If the buffered_replays were non-empty and the replay_type matches, we can buffer in the previous one.
	// Either way, our target for buffering is now the last element of buffered_replays.
	BufferedIndexData &buffer = buffered_replays.back();

	// Initialize the small chunk.
	if (!buffer.small_chunk) {
		buffer.small_chunk = make_uniq<DataChunk>();
		buffer.small_chunk->Initialize(allocator, types, 1);
		buffer.small_chunk->SetCardinality(0);
	}

	if (combined_chunk.size() + buffer.small_chunk->size() < STANDARD_VECTOR_SIZE) {
		// Append to small_chunk and allow resizing, since we know we can fit under STANDARD_VECTOR_SIZE.
		buffer.small_chunk->Append(combined_chunk, true);
	} else {
		// Otherwise we have at least one STANDARD_VECTOR_SIZE chunk we can spill to ColumnDataCollection.

		// Initialize ColumnDataCollection data buffer.
		if (!buffer.data) {
			buffer.data = make_uniq<ColumnDataCollection>(allocator, types);
		}

		// Remainder is the remaining space we want to fill up in the ColumnDataCollection chunk, after writing the
		// small chunk there.
		idx_t remainder = STANDARD_VECTOR_SIZE - buffer.small_chunk->size();
		// First, flush the small_chunk contents into the ColumnDataCollection buffer.
		if (buffer.small_chunk->size() > 0) {
			buffer.data->Append(*buffer.small_chunk);
			buffer.small_chunk->SetCardinality(0);
		}

		idx_t total = combined_chunk.size();
		// Leftover is whatever is leftover after we buffered a full STANDARD_VECTOR_SIZE amount into
		// ColumnDataCollection data -- this goes into small_chunk.
		idx_t leftover = (total - remainder);

		if (remainder > 0) {
			DataChunk remainder_chunk;
			remainder_chunk.InitializeEmpty(types);
			remainder_chunk.Reference(combined_chunk);
			remainder_chunk.Slice(0, remainder);
			remainder_chunk.SetCardinality(remainder);
			buffer.data->Append(remainder_chunk);
		}

		// Buffer any leftover replays into the small chunk.
		if (leftover > 0) {
			DataChunk tail_chunk;
			tail_chunk.InitializeEmpty(types);
			tail_chunk.Reference(combined_chunk);
			tail_chunk.Slice(remainder, leftover);
			tail_chunk.SetCardinality(leftover);
			buffer.small_chunk->Append(tail_chunk, true);
		}
	}
}

} // namespace duckdb
