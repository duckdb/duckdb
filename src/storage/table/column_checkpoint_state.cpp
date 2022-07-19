
#include "duckdb/storage/table/column_data.hpp"

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"
#include "duckdb/storage/table/list_column_data.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

ColumnCheckpointState::ColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, TableDataWriter &writer)
    : row_group(row_group), column_data(column_data), writer(writer) {
}

ColumnCheckpointState::~ColumnCheckpointState() {
}

unique_ptr<BaseStatistics> ColumnCheckpointState::GetStatistics() {
	D_ASSERT(global_stats);
	return move(global_stats);
}

void ColumnCheckpointState::FlushSegment(unique_ptr<ColumnSegment> segment, idx_t segment_size) {
	D_ASSERT(segment_size <= Storage::BLOCK_SIZE);
	auto tuple_count = segment->count.load();
	if (tuple_count == 0) { // LCOV_EXCL_START
		return;
	} // LCOV_EXCL_STOP

	// merge the segment stats into the global stats
	global_stats->Merge(*segment->stats.statistics);

	// get the buffer of the segment and pin it
	auto &db = column_data.GetDatabase();
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto &block_manager = BlockManager::GetBlockManager(db);
	auto &checkpoint_manager = writer.GetCheckpointManager();

	bool block_is_constant = segment->stats.statistics->IsConstant();

	block_id_t block_id = INVALID_BLOCK;
	uint32_t offset_in_block = 0;
	bool need_to_write = true;
	PartialBlock *partial_block = nullptr;
	unique_ptr<PartialBlock> owned_partial_block;
	if (!block_is_constant) {
		// non-constant block
		// if the block is less than 80% full, we consider it a "partial block"
		// which means we will try to fit it with other blocks
		if (segment_size <= CheckpointManager::PARTIAL_BLOCK_THRESHOLD) {
			// the block is a partial block
			// check if there is a partial block available we can write to
			if (checkpoint_manager.GetPartialBlock(segment.get(), segment_size, block_id, offset_in_block,
			                                       partial_block, owned_partial_block)) {
				//! there is! increase the reference count of this block
				block_manager.IncreaseBlockReferenceCount(block_id);
			} else {
				// there isn't: generate a new block for this segment
				block_id = block_manager.GetFreeBlockId();
				offset_in_block = 0;
				need_to_write = false;
				// now register this block as a partial block
				checkpoint_manager.RegisterPartialBlock(segment.get(), segment_size, block_id);
			}
		} else {
			// full block: get a free block to write to
			block_id = block_manager.GetFreeBlockId();
			offset_in_block = 0;
		}
	} else {
		// constant block: no need to write anything to disk besides the stats
		// set up the compression function to constant
		auto &config = DBConfig::GetConfig(db);
		segment->function =
		    config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, segment->type.InternalType());
	}

	// construct the data pointer
	DataPointer data_pointer;
	data_pointer.block_pointer.block_id = block_id;
	data_pointer.block_pointer.offset = offset_in_block;
	data_pointer.row_start = row_group.start;
	if (!data_pointers.empty()) {
		auto &last_pointer = data_pointers.back();
		data_pointer.row_start = last_pointer.row_start + last_pointer.tuple_count;
	}
	data_pointer.tuple_count = tuple_count;
	data_pointer.compression_type = segment->function->type;
	data_pointer.statistics = segment->stats.statistics->Copy();

	if (need_to_write) {
		if (partial_block) {
			// pin the current block
			auto old_handle = buffer_manager.Pin(segment->block);
			// pin the new block
			auto new_handle = buffer_manager.Pin(partial_block->block);
			// memcpy the contents of the old block to the new block
			memcpy(new_handle.Ptr() + offset_in_block, old_handle.Ptr(), segment_size);
		} else {
			// convert the segment into a persistent segment that points to this block
			segment->ConvertToPersistent(block_id);
		}
	}
	if (owned_partial_block) {
		// the partial block has become full: write it to disk
		owned_partial_block->FlushToDisk(db);
	}

	// append the segment to the new segment tree
	new_tree.AppendSegment(move(segment));
	data_pointers.push_back(move(data_pointer));
}

void ColumnCheckpointState::FlushToDisk() {
	auto &meta_writer = writer.GetTableWriter();

	meta_writer.Write<idx_t>(data_pointers.size());
	// then write the data pointers themselves
	for (idx_t k = 0; k < data_pointers.size(); k++) {
		auto &data_pointer = data_pointers[k];
		meta_writer.Write<idx_t>(data_pointer.row_start);
		meta_writer.Write<idx_t>(data_pointer.tuple_count);
		meta_writer.Write<block_id_t>(data_pointer.block_pointer.block_id);
		meta_writer.Write<uint32_t>(data_pointer.block_pointer.offset);
		meta_writer.Write<CompressionType>(data_pointer.compression_type);
		data_pointer.statistics->Serialize(meta_writer);
	}
}

} // namespace duckdb
