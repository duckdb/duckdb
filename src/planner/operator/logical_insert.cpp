#include "duckdb/planner/operator/logical_insert.hpp"

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

unique_ptr<LogicalOperator> LogicalInsert::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                       FieldReader &reader) {
	auto insert_values_size = reader.ReadRequired<idx_t>();
	vector<vector<unique_ptr<Expression>>> insert_values;
	for (idx_t i = 0; i < insert_values_size; i++) {
		insert_values.push_back(reader.ReadRequiredSerializableList<Expression>(context));
	}
	auto column_index_map = reader.ReadRequiredList<idx_t>();
	auto expected_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto table_index = reader.ReadRequired<idx_t>();
	auto return_chunk = reader.ReadRequired<bool>();
	auto bound_defaults = reader.ReadRequiredSerializableList<Expression>(context);

	auto name = reader.ReadRequired<string>();
	auto &catalog = Catalog::GetCatalog(context);

	auto table_catalog = catalog.GetEntry(context, DEFAULT_SCHEMA, name);

	if (!table_catalog || table_catalog->type != CatalogType::TABLE_ENTRY) {
		throw InternalException("Cant find catalog entry for table %s", name);
	}

	auto table = (TableCatalogEntry *)table_catalog;
	auto result = make_unique<LogicalInsert>(table);
	result->type = type;
	result->table = table;
	result->table_index = table_index;
	result->return_chunk = return_chunk;
	result->insert_values = move(insert_values);
	result->column_index_map = column_index_map;
	result->expected_types = expected_types;
	result->bound_defaults = move(bound_defaults);
	return result;
}

} // namespace duckdb
