#include "duckdb/storage/checkpoint/table_data_reader.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
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

	// We stored the table statistics as a unit in FinalizeTable.
	BinaryDeserializer stats_deserializer(reader);
	stats_deserializer.Begin();
	info.data->table_stats.Deserialize(stats_deserializer, columns);
	stats_deserializer.End();

	// Deserialize the row group pointers (lazily, just set the count and the pointer to them for now)
	info.data->row_group_count = reader.Read<uint64_t>();
	info.data->block_pointer = reader.GetMetaBlockPointer();
}

} // namespace duckdb
