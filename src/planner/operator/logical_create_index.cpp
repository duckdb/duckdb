#include "duckdb/planner/operator/logical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

void LogicalCreateIndex::Serialize(FieldWriter &writer) const {
	table.Serialize(writer.GetSerializer());
	writer.WriteList<column_t>(column_ids);
	writer.WriteSerializableList(unbound_expressions);
	writer.WriteOptional(info);
}

unique_ptr<LogicalOperator> LogicalCreateIndex::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                            FieldReader &reader) {
	auto catalog_info = TableCatalogEntry::Deserialize(reader.GetSource());
	auto &catalog = Catalog::GetCatalog(context);
	TableCatalogEntry *table = catalog.GetEntry<TableCatalogEntry>(context, catalog_info->schema, catalog_info->table);

	auto column_ids = reader.ReadRequiredList<column_t>();

	auto unbound_expressions = reader.ReadRequiredSerializableList<Expression>(context);

	unique_ptr<CreateIndexInfo> info;
	info = reader.ReadOptional<CreateIndexInfo>(move(info));

	return make_unique<LogicalCreateIndex>(*table, column_ids, move(unbound_expressions), move(info));
}

} // namespace duckdb
