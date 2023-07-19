#include "duckdb/planner/operator/logical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/function/function_serialization.hpp"

namespace duckdb {

LogicalCreateIndex::LogicalCreateIndex(unique_ptr<FunctionData> bind_data_p, unique_ptr<CreateIndexInfo> info_p,
                                       vector<unique_ptr<Expression>> expressions_p, TableCatalogEntry &table_p,
                                       TableFunction function_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_INDEX), bind_data(std::move(bind_data_p)),
      info(std::move(info_p)), table(table_p), function(std::move(function_p)) {

	for (auto &expr : expressions_p) {
		this->unbound_expressions.push_back(expr->Copy());
	}
	this->expressions = std::move(expressions_p);

	if (info->column_ids.empty()) {
		throw BinderException("CREATE INDEX does not refer to any columns in the base table!");
	}
}

void LogicalCreateIndex::Serialize(FieldWriter &writer) const {
	writer.WriteOptional(info);
	writer.WriteString(table.catalog.GetName());
	writer.WriteString(table.schema.name);
	writer.WriteString(table.name);
	FunctionSerializer::SerializeBase<TableFunction>(writer, function, bind_data.get());
	writer.WriteSerializableList(unbound_expressions);
}

unique_ptr<LogicalOperator> LogicalCreateIndex::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto &context = state.gstate.context;

	auto info = reader.ReadOptional<CreateInfo>(nullptr);
	auto catalog = reader.ReadRequired<string>();
	auto schema = reader.ReadRequired<string>();
	auto table_name = reader.ReadRequired<string>();
	unique_ptr<FunctionData> bind_data;
	bool has_deserialize;
	auto function = FunctionSerializer::DeserializeBaseInternal<TableFunction, TableFunctionCatalogEntry>(
	    reader, state.gstate, CatalogType::TABLE_FUNCTION_ENTRY, bind_data, has_deserialize);
	auto unbound_expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	if (info->type != CatalogType::INDEX_ENTRY) {
		throw InternalException("Unexpected type: '%s', expected '%s'", CatalogTypeToString(info->type),
		                        CatalogTypeToString(CatalogType::INDEX_ENTRY));
	}
	auto index_info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(info));
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table_name);
	return make_uniq<LogicalCreateIndex>(std::move(bind_data), std::move(index_info), std::move(unbound_expressions),
	                                     table, std::move(function));
}

void LogicalCreateIndex::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

} // namespace duckdb
