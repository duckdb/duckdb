#include "duckdb/storage/checkpoint/table_data_reader.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

TableDataReader::TableDataReader(CheckpointManager &manager, MetaBlockReader &reader, BoundCreateTableInfo &info)
    : manager(manager), reader(reader), info(info) {
	info.data = unique_ptr<vector<unique_ptr<PersistentSegment>>[]>(
	    new vector<unique_ptr<PersistentSegment>>[info.Base().columns.size()]);
}

void TableDataReader::ReadTableData() {
	auto &columns = info.Base().columns;
	assert(columns.size() > 0);

	// load the data pointers for the table
	idx_t table_count = 0;
	for (idx_t col = 0; col < columns.size(); col++) {
		auto &column = columns[col];
		idx_t column_count = 0;
		idx_t data_pointer_count = reader.Read<idx_t>();
		for (idx_t data_ptr = 0; data_ptr < data_pointer_count; data_ptr++) {
			// read the data pointer
			DataPointer data_pointer;
			data_pointer.min = reader.Read<double>();
			data_pointer.max = reader.Read<double>();
			data_pointer.row_start = reader.Read<idx_t>();
			data_pointer.tuple_count = reader.Read<idx_t>();
			data_pointer.block_id = reader.Read<block_id_t>();
			data_pointer.offset = reader.Read<uint32_t>();
			reader.ReadData(data_pointer.min_stats, 8);
			reader.ReadData(data_pointer.max_stats, 8);

			column_count += data_pointer.tuple_count;
			// create a persistent segment
			auto segment = make_unique<PersistentSegment>(
			    manager.buffer_manager, data_pointer.block_id, data_pointer.offset, GetInternalType(column.type),
			    data_pointer.row_start, data_pointer.tuple_count, data_pointer.min_stats, data_pointer.max_stats);
			info.data[col].push_back(move(segment));
		}
		if (col == 0) {
			table_count = column_count;
		} else {
			if (table_count != column_count) {
				throw Exception("Column length mismatch in table load!");
			}
		}
	}
}
