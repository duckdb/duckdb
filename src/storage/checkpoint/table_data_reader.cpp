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

	BinaryDeserializer table_deserializer(reader);
	table_deserializer.Begin();

	// Deserialize he table statistics
	table_deserializer.ReadObject(
	    100, "table_stats", [&](FormatDeserializer &obj) { info.data->table_stats.FormatDeserialize(obj, columns); });

	// Deserialize the row group pointers
	info.data->row_group_count = table_deserializer.ReadProperty<uint64_t>(101, "row_group_count");
	info.data->block_pointer = reader.GetMetaBlockPointer();

	// throw InternalException("TODO");
	//	// deserialize the total table statistics
	//	info.data->table_stats.Deserialize(reader, columns);
	//
	//	// deserialize each of the individual row groups
	//	info.data->row_group_count = reader.Read<uint64_t>();
	//	info.data->block_pointer = reader.GetMetaBlockPointer();
}

} // namespace duckdb
