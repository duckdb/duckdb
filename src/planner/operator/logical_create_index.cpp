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

unique_ptr<LogicalOperator> LogicalCreateIndex::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto &context = state.gstate.context;
	auto catalog_info = TableCatalogEntry::Deserialize(reader.GetSource(), context);
	auto &catalog = Catalog::GetCatalog(context);
	TableCatalogEntry *table = catalog.GetEntry<TableCatalogEntry>(context, catalog_info->schema, catalog_info->table);

	auto column_ids = reader.ReadRequiredList<column_t>();

	auto unbound_expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);

	auto create_info = reader.ReadOptional<CreateInfo>(nullptr);
	if (create_info->type != CatalogType::INDEX_ENTRY) {
		throw InternalException("Unexpected type: '%s', expected '%s'", CatalogTypeToString(create_info->type),
		                        CatalogTypeToString(CatalogType::INDEX_ENTRY));
	}

	CreateInfo *raw_create_info_ptr = create_info.release();
	CreateIndexInfo *raw_create_index_info_ptr = static_cast<CreateIndexInfo *>(raw_create_info_ptr);
	unique_ptr<CreateIndexInfo> uptr_create_index_info = unique_ptr<CreateIndexInfo> {raw_create_index_info_ptr};

	auto info = unique_ptr<CreateIndexInfo> {static_cast<CreateIndexInfo *>(create_info.release())};
	return make_unique<LogicalCreateIndex>(*table, column_ids, move(unbound_expressions), move(info));
}

} // namespace duckdb
