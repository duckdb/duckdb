#include "duckdb/storage/checkpoint/table_data_reader.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/common/types/null_value.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "duckdb/main/database.hpp"

namespace duckdb {

TableDataReader::TableDataReader(MetadataReader &reader, BoundCreateTableInfo &info) : reader(reader), info(info) {
	info.data = make_uniq<PersistentTableData>(info.Base().columns.LogicalColumnCount());
}

void TableDataReader::ReadTableData() {
	auto &columns = info.Base().columns;
	D_ASSERT(!columns.empty());

	// deserialize the total table statistics
	info.data->table_stats.Deserialize(reader, columns);

	// deserialize each of the individual row groups
	info.data->row_group_count = reader.Read<uint64_t>();
	info.data->block_pointer = reader.GetMetaBlockPointer();
}

} // namespace duckdb
