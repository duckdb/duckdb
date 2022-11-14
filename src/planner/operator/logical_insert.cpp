#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

void LogicalInsert::Serialize(FieldWriter &writer) const {
	writer.WriteField<idx_t>(insert_values.size());
	for (auto &entry : insert_values) {
		writer.WriteSerializableList(entry);
	}

	writer.WriteList<idx_t>(column_index_map);
	writer.WriteRegularSerializableList(expected_types);
	table->Serialize(writer.GetSerializer());
	writer.WriteField(table_index);
	writer.WriteField(return_chunk);
	writer.WriteSerializableList(bound_defaults);
}

unique_ptr<LogicalOperator> LogicalInsert::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto &context = state.gstate.context;
	auto insert_values_size = reader.ReadRequired<idx_t>();
	vector<vector<unique_ptr<Expression>>> insert_values;
	for (idx_t i = 0; i < insert_values_size; ++i) {
		insert_values.push_back(reader.ReadRequiredSerializableList<Expression>(state.gstate));
	}

	auto column_index_map = reader.ReadRequiredList<idx_t, physical_index_vector_t<idx_t>>();
	auto expected_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto info = TableCatalogEntry::Deserialize(reader.GetSource(), context);
	auto table_index = reader.ReadRequired<idx_t>();
	auto return_chunk = reader.ReadRequired<bool>();
	auto bound_defaults = reader.ReadRequiredSerializableList<Expression>(state.gstate);

	auto &catalog = Catalog::GetCatalog(context);

	TableCatalogEntry *table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(context, info->schema, info->table);

	if (!table_catalog_entry) {
		throw InternalException("Cant find catalog entry for table %s", info->table);
	}

	auto result = make_unique<LogicalInsert>(table_catalog_entry);
	result->type = state.type;
	result->table = table_catalog_entry;
	result->table_index = table_index;
	result->return_chunk = return_chunk;
	result->insert_values = move(insert_values);
	result->column_index_map = column_index_map;
	result->expected_types = expected_types;
	result->bound_defaults = move(bound_defaults);
	return move(result);
}

idx_t LogicalInsert::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

} // namespace duckdb
