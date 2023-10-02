#include "duckdb/planner/operator/logical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/function/function_serialization.hpp"

namespace duckdb {

LogicalCreateIndex::LogicalCreateIndex(unique_ptr<CreateIndexInfo> info_p, vector<unique_ptr<Expression>> expressions_p,
                                       TableCatalogEntry &table_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_INDEX), info(std::move(info_p)), table(table_p) {

	for (auto &expr : expressions_p) {
		this->unbound_expressions.push_back(expr->Copy());
	}
	this->expressions = std::move(expressions_p);

	if (info->column_ids.empty()) {
		throw BinderException("CREATE INDEX does not refer to any columns in the base table!");
	}
}

LogicalCreateIndex::LogicalCreateIndex(ClientContext &context, unique_ptr<CreateInfo> info_p,
                                       vector<unique_ptr<Expression>> expressions_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_INDEX),
      info(unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(info_p))), table(BindTable(context, *info)) {
	for (auto &expr : expressions_p) {
		this->unbound_expressions.push_back(expr->Copy());
	}
	this->expressions = std::move(expressions_p);
}

void LogicalCreateIndex::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

TableCatalogEntry &LogicalCreateIndex::BindTable(ClientContext &context, CreateIndexInfo &info) {
	auto &catalog = info.catalog;
	auto &schema = info.schema;
	auto &table_name = info.table;
	return Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table_name);
}

} // namespace duckdb
