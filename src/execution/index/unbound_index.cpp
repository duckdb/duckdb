#include "duckdb/execution/index/unbound_index.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

static constexpr idx_t RESIZE_THRESHOLD = 128;

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

void UnboundIndex::BufferChunk(DataChunk &index_column_chunk, Vector &row_ids,
                               const vector<StorageIndex> &mapped_column_ids_p, BufferedIndexReplay replay_type) {
	D_ASSERT(!column_ids.empty());
	auto types = index_column_chunk.GetTypes(); // column types
	types.push_back(LogicalType::ROW_TYPE);

	auto &allocator = Allocator::Get(db);

	// If the replay type doesn't match the type of the BufferedIndexData at the end, we append a new
	// BufferedIndexData that we can buffer into.
	if (buffered_replays.empty() || buffered_replays.back().type != replay_type) {
		// Before creating a new BufferedIndexData, resize the previous small_chunk if it has too many free slots.
		if (!buffered_replays.empty() && buffered_replays.back().small_chunk) {
			auto &prev = buffered_replays.back();
			if (prev.small_chunk->size() == 0) {
				// Logically empty - destroy it entirely to free unused space.
				prev.small_chunk.reset();
			} else {
				// Only resize if unused capacity exceeds threshold.
				auto unused_capacity = prev.small_chunk->GetCapacity() - prev.small_chunk->size();
				if (unused_capacity >= RESIZE_THRESHOLD) {
					auto current_size = prev.small_chunk->size();
					auto desired_capacity = MinValue<idx_t>(STANDARD_VECTOR_SIZE, current_size);
					auto old_chunk = std::move(prev.small_chunk);
					prev.small_chunk = make_uniq<DataChunk>();
					prev.small_chunk->Initialize(allocator, types, desired_capacity);
					prev.small_chunk->Append(*old_chunk, false);
				}
			}
		}
		BufferedIndexData buffered_data(replay_type);
		buffered_replays.emplace_back(std::move(buffered_data));
	}
	// If it did match, we can buffer into the previous one.
	// Either way, our target for buffering is now the last element of the buffered_replays.
	BufferedIndexData &target = buffered_replays.back();

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

	// Initialize the small chunk.
	if (!target.small_chunk) {
		target.small_chunk = make_uniq<DataChunk>();
		target.small_chunk->Initialize(allocator, types, 1);
		target.small_chunk->SetCardinality(0);
	}

	if (combined_chunk.size() + target.small_chunk->size() < STANDARD_VECTOR_SIZE) {
		// Append to small chunk and allow resizing, since we know we can fit into STANDARD_VECTOR_SIZE, i.e.,
		// if the small chunk is already at STANDARD_VECTOR_SIZE capacity, it won't get resized higher since the data
		// will fit.
		target.small_chunk->Append(combined_chunk, true);
	} else {
		if (!target.data) {
			target.data = make_uniq<ColumnDataCollection>(allocator, types);
		}
		idx_t remainder = STANDARD_VECTOR_SIZE - target.small_chunk->size();

		// First, flush the small_chunk contents into ColumnDataCollection buffer.
		if (target.small_chunk->size() > 0) {
			target.data->Append(*target.small_chunk);
			target.small_chunk->SetCardinality(0);
		}
		// Write remainder columns to ColumnDataCollection to fill up to STANDARD_VECTOR_SIZE.
		// Since combined_chunk.size() + target.small_chunk->size() >= STANDARD_VECTOR_SIZE, we are guaranteed to
		// fill it up in the ColumnDataCollection.
		if (remainder > 0) {
			DataChunk remainder_chunk;
			remainder_chunk.InitializeEmpty(types);
			remainder_chunk.Reference(combined_chunk);
			remainder_chunk.Slice(0, remainder);
			remainder_chunk.SetCardinality(remainder);
			target.data->Append(remainder_chunk);
		}
		// Now start writing STANDARD_VECTOR_SIZE chunks from combined_chunk.
		idx_t offset = remainder;
		while (offset + STANDARD_VECTOR_SIZE <= combined_chunk.size()) {
			DataChunk block_chunk;
			block_chunk.InitializeEmpty(types);
			block_chunk.Reference(combined_chunk);
			block_chunk.Slice(offset, STANDARD_VECTOR_SIZE);
			block_chunk.SetCardinality(STANDARD_VECTOR_SIZE);
			target.data->Append(block_chunk);
			offset += STANDARD_VECTOR_SIZE;
		}

		// Buffer any leftover rows back into the small chunk.
		// By this point, small_chunk has been flushed and its cardinality set to 0.
		idx_t leftover = combined_chunk.size() - offset;
		if (leftover > 0) {
			DataChunk tail_chunk;
			tail_chunk.InitializeEmpty(types);
			tail_chunk.Reference(combined_chunk);
			tail_chunk.Slice(offset, leftover);
			tail_chunk.SetCardinality(leftover);
			target.small_chunk->Append(tail_chunk, true);
		}
	}
}

} // namespace duckdb
