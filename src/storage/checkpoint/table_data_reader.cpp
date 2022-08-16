#include "duckdb/storage/checkpoint/table_data_reader.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/storage/table/row_group.hpp"

namespace duckdb {

TableDataReader::TableDataReader(MetaBlockReader &reader, BoundCreateTableInfo &info) : reader(reader), info(info) {
	info.data = make_unique<PersistentTableData>(info.Base().columns.size());
}

void TableDataReader::ReadTableData() {
	auto &columns = info.Base().columns;
	D_ASSERT(!columns.empty());

	// deserialize the total table statistics
	info.data->column_stats.reserve(columns.size());
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &col = columns[i];
		// Have to use 'Generated()' here, storage_oid is uninitialized here
		if (col.Generated()) {
			continue;
		}
		info.data->column_stats.push_back(BaseStatistics::Deserialize(reader, columns[i].Type()));
	}

	// deserialize each of the individual row groups
	auto row_group_count = reader.Read<uint64_t>();
	info.data->row_groups.reserve(row_group_count);
	for (idx_t i = 0; i < row_group_count; i++) {
		auto row_group_pointer = RowGroup::Deserialize(reader, columns);
		info.data->row_groups.push_back(move(row_group_pointer));
	}
}

} // namespace duckdb
