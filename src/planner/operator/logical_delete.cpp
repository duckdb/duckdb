#include "duckdb/planner/operator/logical_delete.hpp"

namespace duckdb {

void LogicalDelete::Serialize(FieldWriter &writer) const {
	table->Serialize(writer.GetSerializer());
	writer.WriteField(table_index);
	writer.WriteField(return_chunk);
}

unique_ptr<LogicalOperator> LogicalDelete::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                       FieldReader &reader) {
	auto info = TableCatalogEntry::Deserialize(reader.GetSource());

	auto &catalog = Catalog::GetCatalog(context);

	TableCatalogEntry *table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(context, info->schema, info->table);

	auto result = make_unique<LogicalDelete>(table_catalog_entry);
	result->table_index = reader.ReadRequired<idx_t>();
	result->return_chunk = reader.ReadRequired<bool>();
	return result;
}

} // namespace duckdb
