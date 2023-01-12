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

	auto table_catalog_entry =
	    Catalog::GetEntry<TableCatalogEntry>(context, INVALID_CATALOG, info->schema, info->table);

	auto table_index = reader.ReadRequired<idx_t>();
	auto result = make_unique<LogicalDelete>(table_catalog_entry, table_index);
	result->return_chunk = reader.ReadRequired<bool>();
	return std::move(result);
}

idx_t LogicalDelete::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

vector<idx_t> LogicalDelete::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
