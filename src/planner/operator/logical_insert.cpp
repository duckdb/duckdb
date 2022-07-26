#include "duckdb/planner/operator/logical_insert.hpp"

namespace duckdb {

void LogicalInsert::Serialize(FieldWriter &writer) const {
	writer.WriteField<idx_t>(insert_values.size());
	for (auto &entry : insert_values) {
		writer.WriteSerializableList(entry);
	}
	writer.WriteList<idx_t>(column_index_map);
	writer.WriteRegularSerializableList(expected_types);
	writer.WriteField(table_index);
	writer.WriteField(return_chunk);
	writer.WriteSerializableList(bound_defaults);

	// The base table (TableCatalogEntry) to insert into
	writer.WriteString(table->schema->name);
	writer.WriteString(table->name);
	writer.WriteRegularSerializableList(table->columns);
	writer.WriteSerializableList(table->constraints);
}

unique_ptr<LogicalOperator> LogicalInsert::Deserialize(FieldReader &source, ClientContext &context) {
	auto insert_values_size = source.ReadRequired<idx_t>();
	vector<vector<unique_ptr<Expression>>> insert_values;
	for (idx_t i = 0; i < insert_values_size; i++) {
		insert_values.push_back(source.ReadRequiredList<unique_ptr<Expression>>());
	}
	auto column_index_map = source.ReadRequired<idx_t>();
	auto expected_types = source.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto table_index = source.ReadRequired<idx_t>();
	auto return_chunk = source.ReadRequired<bool>();
	auto bound_defaults = source.ReadRequiredSerializableList<Expression>();

	auto name = source.ReadRequired<string>();
	auto &catalog = context.db->GetCatalog();

	auto table_catalog = catalog.GetEntry(context, DEFAULT_SCHEMA, name);

	if (!table_catalog || table_catalog->type != CatalogType::TABLE_ENTRY) {
		throw InternalException("Cant find catalog entry for table %s", name);
	}

	auto table = (TableCatalogEntry *)table_catalog;
	auto result = make_unique<LogicalInsert>(table);
	result->type = LogicalOperatorType::LOGICAL_INSERT;
	result->table = table;
	result->table_index = table_index;
	result->return_chunk = return_chunk;
	return result;
}

unique_ptr<LogicalOperator> LogicalInsert::Deserialize(ClientContext &context, LogicalOperatorType type,
                                                       FieldReader &reader) {
	throw NotImplementedException(LogicalOperatorToString(type));
}

} // namespace duckdb
