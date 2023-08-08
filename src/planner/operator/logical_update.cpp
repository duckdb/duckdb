#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

LogicalUpdate::LogicalUpdate(TableCatalogEntry &table)
    : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE), table(table), table_index(0), return_chunk(false) {
}

LogicalUpdate::LogicalUpdate(ClientContext &context, const string &catalog, const string &schema, const string &table)
    : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE),
      table(Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table)) {
}

void LogicalUpdate::Serialize(FieldWriter &writer) const {
	table.Serialize(writer.GetSerializer());
	writer.WriteField(table_index);
	writer.WriteField(return_chunk);
	writer.WriteIndexList<PhysicalIndex>(columns);
	writer.WriteSerializableList(bound_defaults);
	writer.WriteField(update_is_del_and_insert);
	writer.WriteSerializableList(this->expressions);
}

unique_ptr<LogicalOperator> LogicalUpdate::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto &context = state.gstate.context;
	auto info = TableCatalogEntry::Deserialize(reader.GetSource());
	auto &catalog = Catalog::GetCatalog(context, info->catalog);

	auto &table_catalog_entry =
	    catalog.GetEntry<TableCatalogEntry>(context, info->schema, info->Cast<CreateTableInfo>().table);
	auto result = make_uniq<LogicalUpdate>(table_catalog_entry);
	result->table_index = reader.ReadRequired<idx_t>();
	result->return_chunk = reader.ReadRequired<bool>();
	result->columns = reader.ReadRequiredIndexList<PhysicalIndex>();
	result->bound_defaults = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	result->update_is_del_and_insert = reader.ReadRequired<bool>();
	result->expressions = reader.ReadRequiredSerializableList<duckdb::Expression>(state.gstate);
	return std::move(result);
}

idx_t LogicalUpdate::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

vector<ColumnBinding> LogicalUpdate::GetColumnBindings() {
	if (return_chunk) {
		return GenerateColumnBindings(table_index, table.GetTypes().size());
	}
	return {ColumnBinding(0, 0)};
}

void LogicalUpdate::ResolveTypes() {
	if (return_chunk) {
		types = table.GetTypes();
	} else {
		types.emplace_back(LogicalType::BIGINT);
	}
}

string LogicalUpdate::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
