
#include "duckdb/storage/table/column_data.hpp"

#include "duckdb/storage/segment/compressed_segment.hpp"
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

void ColumnCheckpointState::CreateEmptySegment() {
	auto &db = column_data.GetDatabase();
	auto type_id = column_data.type.InternalType();
	auto &config = DBConfig::GetConfig(db);
	current_segment = make_unique<CompressedSegment>(db, type_id, row_group.start, config.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, type_id));
	segment_stats = make_unique<SegmentStatistics>(column_data.type);
}

void ColumnCheckpointState::AppendData(Vector &data, idx_t count) {
	VectorData vdata;
	data.Orrify(count, vdata);

	idx_t offset = 0;
	while (count > 0) {
		auto &uncompressed = (BaseSegment &) *current_segment;
		idx_t appended = uncompressed.Append(*segment_stats, vdata, offset, count);
		if (appended == count) {
			// appended everything: finished
			return;
		}
		// the segment is full: flush it to disk
		FlushSegment(*current_segment, move(segment_stats->statistics));
		current_segment.reset();
		segment_stats.reset();

		// now create a new segment and continue appending
		CreateEmptySegment();
		offset += appended;
		count -= appended;
	}
}

void ColumnCheckpointState::FlushSegment(BaseSegment &segment, unique_ptr<BaseStatistics> stats) {
	auto tuple_count = segment.tuple_count.load();
	if (tuple_count == 0) {
		return;
	}

	// get the buffer of the segment and pin it
	auto &buffer_manager = BufferManager::GetBufferManager(column_data.GetDatabase());
	auto &block_manager = BlockManager::GetBlockManager(column_data.GetDatabase());

	bool block_is_constant = stats->IsConstant();

	block_id_t block_id;
	uint32_t offset_in_block;
	if (!block_is_constant) {
		// get a free block id to write to
		block_id = block_manager.GetFreeBlockId();
		offset_in_block = 0;
	} else {
		block_id = INVALID_BLOCK;
		offset_in_block = 0;
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
	data_pointer.statistics = stats->Copy();

	// construct a persistent segment that points to this block, and append it to the new segment tree
	auto persistent_segment = make_unique<PersistentSegment>(
	    column_data.GetDatabase(), block_id, offset_in_block, column_data.type, data_pointer.row_start,
	    data_pointer.tuple_count, stats->Copy());
	new_tree.AppendSegment(move(persistent_segment));

	data_pointers.push_back(move(data_pointer));

	if (!block_is_constant) {
		// write the block to disk
		auto handle = buffer_manager.Pin(segment.block);
		block_manager.Write(*handle->node, block_id);
		handle.reset();
	}

	// merge the segment stats into the global stats
	global_stats->Merge(*stats);
}

void ColumnCheckpointState::FlushToDisk() {
	auto &meta_writer = writer.GetMetaWriter();

	meta_writer.Write<idx_t>(data_pointers.size());
	// then write the data pointers themselves
	for (idx_t k = 0; k < data_pointers.size(); k++) {
		auto &data_pointer = data_pointers[k];
		meta_writer.Write<idx_t>(data_pointer.row_start);
		meta_writer.Write<idx_t>(data_pointer.tuple_count);
		meta_writer.Write<block_id_t>(data_pointer.block_pointer.block_id);
		meta_writer.Write<uint32_t>(data_pointer.block_pointer.offset);
		data_pointer.statistics->Serialize(meta_writer);
	}
}

}
