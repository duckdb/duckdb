#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"

#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/string_segment.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/storage/column_data.hpp"

namespace duckdb {

class WriteOverflowStringsToDisk : public OverflowStringWriter {
public:
	WriteOverflowStringsToDisk(DatabaseInstance &db);
	~WriteOverflowStringsToDisk();

	//! The checkpoint manager
	DatabaseInstance &db;

	//! Temporary buffer
	unique_ptr<BufferHandle> handle;
	//! The block on-disk to which we are writing
	block_id_t block_id;
	//! The offset within the current block
	idx_t offset;

	static constexpr idx_t STRING_SPACE = Storage::BLOCK_SIZE - sizeof(block_id_t);

public:
	void WriteString(string_t string, block_id_t &result_block, int32_t &result_offset) override;

private:
	void AllocateNewBlock(block_id_t new_block_id);
};

TableDataWriter::TableDataWriter(DatabaseInstance &db, TableCatalogEntry &table, MetaBlockWriter &meta_writer)
    : db(db), table(table), meta_writer(meta_writer) {
}

TableDataWriter::~TableDataWriter() {
}

void TableDataWriter::WriteTableData() {
	// allocate the initial segments
	segments.resize(table.columns.size());
	data_pointers.resize(table.columns.size());
	stats.reserve(table.columns.size());
	column_stats.reserve(table.columns.size());
	for (idx_t i = 0; i < table.columns.size(); i++) {
		auto type_id = table.columns[i].type.InternalType();
		stats.push_back(make_unique<SegmentStatistics>(table.columns[i].type, GetTypeIdSize(type_id)));
		column_stats.push_back(BaseStatistics::CreateEmpty(table.columns[i].type));
		CreateSegment(i);
	}

	// now start scanning the table and append the data to the uncompressed segments
	table.storage->Checkpoint(*this);

	VerifyDataPointers();
	WriteDataPointers();
}

void TableDataWriter::CreateSegment(idx_t col_idx) {
	auto type_id = table.columns[col_idx].type.InternalType();
	if (type_id == PhysicalType::VARCHAR) {
		auto string_segment = make_unique<StringSegment>(db, 0);
		string_segment->overflow_writer = make_unique<WriteOverflowStringsToDisk>(db);
		segments[col_idx] = move(string_segment);
	} else {
		segments[col_idx] = make_unique<NumericSegment>(db, type_id, 0);
	}
}

void TableDataWriter::CheckpointColumn(ColumnData &col_data, idx_t col_idx) {
	Vector intermediate(col_data.type);

	// scan the segments of the column data
	// we create a new segment tree with all the new segments
	SegmentTree new_tree;

	auto owned_segment = move(col_data.data.root_node);
	auto segment = (ColumnSegment *) owned_segment.get();
	DataPointer pointer;
	while(segment) {
		switch(segment->segment_type) {
		case ColumnSegmentType::PERSISTENT: {
			// already persisted: no need to write the data

			// flush any segments preceding this persistent segment
			FlushSegment(new_tree, col_idx);

			// set up the data pointer directly using the data from the persistent segment
			auto &persistent = (PersistentSegment&) *segment;
			pointer.block_id = persistent.block_id;
			pointer.offset = 0;
			pointer.row_start = segment->start;
			pointer.tuple_count = persistent.count;
			pointer.statistics = persistent.stats.statistics->Copy();

			// merge the persistent stats into the global column stats
			column_stats[col_idx]->Merge(*persistent.stats.statistics);

			// directly append the current segment to the new tree
			new_tree.AppendSegment(move(owned_segment));
			break;
		}
		case ColumnSegmentType::TRANSIENT: {
			// not persisted yet: scan the segment and write it to disk
			auto &transient = (TransientSegment &) *segment;
			ColumnScanState state;
			transient.InitializeScan(state);
			for(idx_t vector_index = 0; vector_index * STANDARD_VECTOR_SIZE < transient.count; vector_index++) {
				idx_t count = MinValue<idx_t>(transient.count - vector_index * STANDARD_VECTOR_SIZE, STANDARD_VECTOR_SIZE);
				transient.ScanCommitted(state, vector_index, intermediate);
				AppendData(new_tree, col_idx, intermediate, count);
			}
			break;
		}
		}
		// move to the next segment in the list
		owned_segment = move(segment->next);
		segment = (ColumnSegment *) owned_segment.get();
	}
	// flush the final segment
	FlushSegment(new_tree, col_idx);
	// replace the old tree with the new one
	col_data.data.Replace(new_tree);
}

void TableDataWriter::AppendData(SegmentTree &new_tree, idx_t col_idx, Vector &data, idx_t count) {
	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = segments[col_idx]->Append(*stats[col_idx], data, offset, count);
		if (appended == count) {
			// appended everything: finished
			return;
		}
		// the segment is full: flush it to disk
		FlushSegment(new_tree, col_idx);

		// now create a new segment and continue appending
		CreateSegment(col_idx);
		offset += appended;
		count -= appended;
	}
}

void TableDataWriter::FlushSegment(SegmentTree &new_tree, idx_t col_idx) {
	auto tuple_count = segments[col_idx]->tuple_count;
	if (tuple_count == 0) {
		return;
	}

	// get the buffer of the segment and pin it
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto &block_manager = BlockManager::GetBlockManager(db);

	auto handle = buffer_manager.Pin(segments[col_idx]->block);

	// get a free block id to write to
	auto block_id = block_manager.GetFreeBlockId();

	// construct the data pointer
	uint32_t offset_in_block = 0;

	DataPointer data_pointer;
	data_pointer.block_id = block_id;
	data_pointer.offset = offset_in_block;
	data_pointer.row_start = 0;
	if (data_pointers[col_idx].size() > 0) {
		auto &last_pointer = data_pointers[col_idx].back();
		data_pointer.row_start = last_pointer.row_start + last_pointer.tuple_count;
	}
	data_pointer.tuple_count = tuple_count;
	data_pointer.statistics = stats[col_idx]->statistics->Copy();

	// construct a persistent segment that points to this block, and append it to the new segment tree
	auto persistent_segment = make_unique<PersistentSegment>(db, block_id, offset_in_block, table.columns[col_idx].type, data_pointer.row_start, data_pointer.tuple_count, stats[col_idx]->statistics->Copy());
	new_tree.AppendSegment(move(persistent_segment));

	data_pointers[col_idx].push_back(move(data_pointer));
	// write the block to disk
	block_manager.Write(*handle->node, block_id);

	column_stats[col_idx]->Merge(*stats[col_idx]->statistics);
	stats[col_idx] = make_unique<SegmentStatistics>(table.columns[col_idx].type,
	                                                GetTypeIdSize(table.columns[col_idx].type.InternalType()));
	handle.reset();
	segments[col_idx] = nullptr;
}

void TableDataWriter::VerifyDataPointers() {
	// verify the data pointers
	idx_t table_count = 0;
	for (idx_t i = 0; i < data_pointers.size(); i++) {
		auto &data_pointer_list = data_pointers[i];
		idx_t column_count = 0;
		// then write the data pointers themselves
		for (idx_t k = 0; k < data_pointer_list.size(); k++) {
			auto &data_pointer = data_pointer_list[k];
			column_count += data_pointer.tuple_count;
		}
		if (segments[i]) {
			column_count += segments[i]->tuple_count;
		}
		if (i == 0) {
			table_count = column_count;
		} else {
			if (table_count != column_count) {
				throw Exception("Column count mismatch in data write!");
			}
		}
	}
}

void TableDataWriter::WriteDataPointers() {
	for (auto &stats : column_stats) {
		stats->Serialize(meta_writer);
	}

	for (idx_t i = 0; i < data_pointers.size(); i++) {
		// get a reference to the data column
		auto &data_pointer_list = data_pointers[i];
		meta_writer.Write<idx_t>(data_pointer_list.size());
		// then write the data pointers themselves
		for (idx_t k = 0; k < data_pointer_list.size(); k++) {
			auto &data_pointer = data_pointer_list[k];
			meta_writer.Write<idx_t>(data_pointer.row_start);
			meta_writer.Write<idx_t>(data_pointer.tuple_count);
			meta_writer.Write<block_id_t>(data_pointer.block_id);
			meta_writer.Write<uint32_t>(data_pointer.offset);
			data_pointer.statistics->Serialize(meta_writer);
		}
	}
}

WriteOverflowStringsToDisk::WriteOverflowStringsToDisk(DatabaseInstance &db)
    : db(db), block_id(INVALID_BLOCK), offset(0) {
}

WriteOverflowStringsToDisk::~WriteOverflowStringsToDisk() {
	auto &block_manager = BlockManager::GetBlockManager(db);
	if (offset > 0) {
		block_manager.Write(*handle->node, block_id);
	}
}

void WriteOverflowStringsToDisk::WriteString(string_t string, block_id_t &result_block, int32_t &result_offset) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto &block_manager = BlockManager::GetBlockManager(db);
	if (!handle) {
		handle = buffer_manager.Allocate(Storage::BLOCK_ALLOC_SIZE);
	}
	// first write the length of the string
	if (block_id == INVALID_BLOCK || offset + sizeof(uint32_t) >= STRING_SPACE) {
		AllocateNewBlock(block_manager.GetFreeBlockId());
	}
	result_block = block_id;
	result_offset = offset;

	// write the length field
	auto string_length = string.GetSize();
	Store<uint32_t>(string_length, handle->node->buffer + offset);
	offset += sizeof(uint32_t);
	// now write the remainder of the string
	auto strptr = string.GetDataUnsafe();
	uint32_t remaining = string_length;
	while (remaining > 0) {
		uint32_t to_write = MinValue<uint32_t>(remaining, STRING_SPACE - offset);
		if (to_write > 0) {
			memcpy(handle->node->buffer + offset, strptr, to_write);

			remaining -= to_write;
			offset += to_write;
			strptr += to_write;
		}
		if (remaining > 0) {
			// there is still remaining stuff to write
			// first get the new block id and write it to the end of the previous block
			auto new_block_id = block_manager.GetFreeBlockId();
			Store<block_id_t>(new_block_id, handle->node->buffer + offset);
			// now write the current block to disk and allocate a new block
			AllocateNewBlock(new_block_id);
		}
	}
}

void WriteOverflowStringsToDisk::AllocateNewBlock(block_id_t new_block_id) {
	auto &block_manager = BlockManager::GetBlockManager(db);
	if (block_id != INVALID_BLOCK) {
		// there is an old block, write it first
		block_manager.Write(*handle->node, block_id);
	}
	offset = 0;
	block_id = new_block_id;
}

} // namespace duckdb
