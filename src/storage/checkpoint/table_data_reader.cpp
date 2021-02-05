#include "duckdb/storage/checkpoint/table_data_reader.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/storage/table/morsel_info.hpp"

namespace duckdb {

TableDataReader::TableDataReader(DatabaseInstance &db, MetaBlockReader &reader, BoundCreateTableInfo &info)
    : db(db), reader(reader), info(info) {
	info.data = make_unique<PersistentTableData>(info.Base().columns.size());
}

void TableDataReader::ReadTableData() {
	auto &columns = info.Base().columns;
	D_ASSERT(columns.size() > 0);

	// load the column statistics
	for (idx_t col = 0; col < columns.size(); col++) {
		auto &column = columns[col];
		info.data->column_stats[col] = BaseStatistics::Deserialize(reader, column.type);
	}

	// load the data pointers for the table
	idx_t table_count = 0;
	for (idx_t col = 0; col < columns.size(); col++) {
		auto &column = columns[col];
		idx_t column_count = 0;
		idx_t data_pointer_count = reader.Read<idx_t>();
		for (idx_t data_ptr = 0; data_ptr < data_pointer_count; data_ptr++) {
			// read the data pointer
			DataPointer data_pointer;
			data_pointer.row_start = reader.Read<idx_t>();
			data_pointer.tuple_count = reader.Read<idx_t>();
			data_pointer.block_id = reader.Read<block_id_t>();
			data_pointer.offset = reader.Read<uint32_t>();
			data_pointer.statistics = BaseStatistics::Deserialize(reader, column.type);

			column_count += data_pointer.tuple_count;
			// create a persistent segment
			auto segment = make_unique<PersistentSegment>(db, data_pointer.block_id, data_pointer.offset, column.type,
			                                              data_pointer.row_start, data_pointer.tuple_count,
			                                              move(data_pointer.statistics));
			info.data->table_data[col].push_back(move(segment));
		}
		if (col == 0) {
			table_count = column_count;
		} else {
			if (table_count != column_count) {
				throw Exception("Column length mismatch in table load!");
			}
		}
	}
	auto total_rows = table_count;

	// create the version tree
	info.data->versions = make_shared<SegmentTree>();
	for (idx_t i = 0; i < total_rows; i += MorselInfo::MORSEL_SIZE) {
		auto segment = make_unique<MorselInfo>(i, MorselInfo::MORSEL_SIZE);
		// check how many chunk infos we need to read
		auto chunk_info_count = reader.Read<idx_t>();
		if (chunk_info_count > 0) {
			segment->root = make_unique<VersionNode>();
			for (idx_t i = 0; i < chunk_info_count; i++) {
				idx_t vector_index = reader.Read<idx_t>();
				segment->root->info[vector_index] = ChunkInfo::Deserialize(*segment, reader);
			}
		}
		info.data->versions->AppendSegment(move(segment));
	}
}

} // namespace duckdb
