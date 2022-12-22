#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

void LogicalUpdate::Serialize(FieldWriter &writer) const {
	table->Serialize(writer.GetSerializer());
	writer.WriteField(table_index);
	writer.WriteField(return_chunk);
	writer.WriteIndexList<PhysicalIndex>(columns);
	writer.WriteSerializableList(bound_defaults);
	writer.WriteField(update_is_del_and_insert);
}

unique_ptr<LogicalOperator> LogicalUpdate::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto &context = state.gstate.context;
	auto info = TableCatalogEntry::Deserialize(reader.GetSource(), context);
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);

	TableCatalogEntry *table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(context, info->schema, info->table);

	if (!table_catalog_entry) {
		throw InternalException("Cant find catalog entry for table %s", info->table);
	}

	auto result = make_unique<LogicalUpdate>(table_catalog_entry);
	result->table_index = reader.ReadRequired<idx_t>();
	result->return_chunk = reader.ReadRequired<bool>();
	result->columns = reader.ReadRequiredIndexList<PhysicalIndex>();
	result->bound_defaults = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	result->update_is_del_and_insert = reader.ReadRequired<bool>();
	return move(result);
}

idx_t LogicalUpdate::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

} // namespace duckdb
