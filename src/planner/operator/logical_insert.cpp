#include "duckdb/planner/operator/logical_insert.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

LogicalInsert::LogicalInsert(TableCatalogEntry &table, idx_t table_index)
    : LogicalOperator(LogicalOperatorType::LOGICAL_INSERT), table(table), table_index(table_index), return_chunk(false),
      action_type(OnConflictAction::THROW) {
}

LogicalInsert::LogicalInsert(ClientContext &context, const string &catalog, const string &schema, const string &table)
    : LogicalOperator(LogicalOperatorType::LOGICAL_INSERT),
      table(Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table)) {
}

void LogicalInsert::Serialize(FieldWriter &writer) const {
	writer.WriteField<idx_t>(insert_values.size());
	for (auto &entry : insert_values) {
		writer.WriteSerializableList(entry);
	}

	writer.WriteList<idx_t>(column_index_map);
	writer.WriteRegularSerializableList(expected_types);
	table.Serialize(writer.GetSerializer());
	writer.WriteField(table_index);
	writer.WriteField(return_chunk);
	writer.WriteSerializableList(bound_defaults);
	writer.WriteField(action_type);
	writer.WriteRegularSerializableList(expected_set_types);
	writer.WriteList<column_t>(on_conflict_filter);
	writer.WriteOptional(on_conflict_condition);
	writer.WriteOptional(do_update_condition);
	writer.WriteIndexList(set_columns);
	writer.WriteRegularSerializableList(set_types);
	writer.WriteField(excluded_table_index);
	writer.WriteList<column_t>(columns_to_fetch);
	writer.WriteList<column_t>(source_columns);
	writer.WriteSerializableList<Expression>(expressions);
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
	auto info = TableCatalogEntry::Deserialize(reader.GetSource());
	auto table_index = reader.ReadRequired<idx_t>();
	auto return_chunk = reader.ReadRequired<bool>();
	auto bound_defaults = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	auto action_type = reader.ReadRequired<OnConflictAction>();
	auto expected_set_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto on_conflict_filter = reader.ReadRequiredSet<column_t, unordered_set<column_t>>();
	auto on_conflict_condition = reader.ReadOptional<Expression>(nullptr, state.gstate);
	auto do_update_condition = reader.ReadOptional<Expression>(nullptr, state.gstate);
	auto set_columns = reader.ReadRequiredIndexList<PhysicalIndex>();
	auto set_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto excluded_table_index = reader.ReadRequired<idx_t>();
	auto columns_to_fetch = reader.ReadRequiredList<column_t>();
	auto source_columns = reader.ReadRequiredList<column_t>();
	auto expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);

	auto &catalog = Catalog::GetCatalog(context, info->catalog);

	auto &table_catalog_entry =
	    catalog.GetEntry<TableCatalogEntry>(context, info->schema, info->Cast<CreateTableInfo>().table);
	auto result = make_uniq<LogicalInsert>(table_catalog_entry, table_index);
	result->type = state.type;
	result->return_chunk = return_chunk;
	result->insert_values = std::move(insert_values);
	result->column_index_map = column_index_map;
	result->expected_types = expected_types;
	result->bound_defaults = std::move(bound_defaults);
	result->action_type = action_type;
	result->expected_set_types = std::move(expected_set_types);
	result->on_conflict_filter = std::move(on_conflict_filter);
	result->on_conflict_condition = std::move(on_conflict_condition);
	result->do_update_condition = std::move(do_update_condition);
	result->set_columns = std::move(set_columns);
	result->set_types = std::move(set_types);
	result->excluded_table_index = excluded_table_index;
	result->columns_to_fetch = std::move(columns_to_fetch);
	result->source_columns = std::move(source_columns);
	result->expressions = std::move(expressions);
	return std::move(result);
}

idx_t LogicalInsert::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

vector<idx_t> LogicalInsert::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

vector<ColumnBinding> LogicalInsert::GetColumnBindings() {
	if (return_chunk) {
		return GenerateColumnBindings(table_index, table.GetTypes().size());
	}
	return {ColumnBinding(0, 0)};
}

void LogicalInsert::ResolveTypes() {
	if (return_chunk) {
		types = table.GetTypes();
	} else {
		types.emplace_back(LogicalType::BIGINT);
	}
}

string LogicalInsert::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
