#include "duckdb/planner/operator/logical_delete.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

LogicalDelete::LogicalDelete(TableCatalogEntry &table, idx_t table_index)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DELETE), table(table), table_index(table_index),
      return_chunk(false) {
}

void LogicalDelete::Serialize(FieldWriter &writer) const {
	table.Serialize(writer.GetSerializer());
	writer.WriteField(table_index);
	writer.WriteField(return_chunk);
}

unique_ptr<LogicalOperator> LogicalDelete::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto &context = state.gstate.context;
	auto info = TableCatalogEntry::Deserialize(reader.GetSource(), context);

	auto &table_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(context, info->catalog, info->schema, info->table);

	auto table_index = reader.ReadRequired<idx_t>();
	auto result = make_uniq<LogicalDelete>(table_catalog_entry, table_index);
	result->return_chunk = reader.ReadRequired<bool>();
	return std::move(result);
}

idx_t LogicalDelete::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

vector<idx_t> LogicalDelete::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

vector<ColumnBinding> LogicalDelete::GetColumnBindings() {
	if (return_chunk) {
		return GenerateColumnBindings(table_index, table.GetTypes().size());
	}
	return {ColumnBinding(0, 0)};
}

void LogicalDelete::ResolveTypes() {
	if (return_chunk) {
		types = table.GetTypes();
	} else {
		types.emplace_back(LogicalType::BIGINT);
	}
}

} // namespace duckdb
