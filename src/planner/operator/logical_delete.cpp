#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

void LogicalDelete::Serialize(FieldWriter &writer) const {
	table->Serialize(writer.GetSerializer());
	writer.WriteField(table_index);
	writer.WriteField(return_chunk);
}

unique_ptr<LogicalOperator> LogicalDelete::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto &context = state.gstate.context;
	auto info = TableCatalogEntry::Deserialize(reader.GetSource(), context);

	auto &catalog = Catalog::GetCatalog(context);

	TableCatalogEntry *table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(context, info->schema, info->table);

	auto result = make_unique<LogicalDelete>(table_catalog_entry);
	result->table_index = reader.ReadRequired<idx_t>();
	result->return_chunk = reader.ReadRequired<bool>();
	return move(result);
}

idx_t LogicalDelete::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

} // namespace duckdb
