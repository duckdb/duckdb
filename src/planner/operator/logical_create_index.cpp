#include "duckdb/planner/operator/logical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/function/function_serialization.hpp"

namespace duckdb {

LogicalCreateIndex::LogicalCreateIndex(unique_ptr<CreateIndexInfo> info_p, vector<unique_ptr<Expression>> expressions_p,
                                       TableCatalogEntry &table_p, unique_ptr<AlterTableInfo> alter_table_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_INDEX), info(std::move(info_p)), table(table_p),
      alter_table_info(std::move(alter_table_info)) {
	for (auto &expr : expressions_p) {
		unbound_expressions.push_back(expr->Copy());
	}
	expressions = std::move(expressions_p);

	if (info->column_ids.empty()) {
		throw BinderException("CREATE INDEX does not refer to any columns in the base table!");
	}
}

LogicalCreateIndex::LogicalCreateIndex(ClientContext &context, unique_ptr<CreateInfo> info_p,
                                       vector<unique_ptr<Expression>> expressions_p,
                                       unique_ptr<ParseInfo> alter_table_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_INDEX),
      info(unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(info_p))), table(BindTable(context, *info)),
      alter_table_info(unique_ptr_cast<ParseInfo, AlterTableInfo>(std::move(alter_table_info))) {
	for (auto &expr : expressions_p) {
		unbound_expressions.push_back(expr->Copy());
	}
	expressions = std::move(expressions_p);
}

void LogicalCreateIndex::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

TableCatalogEntry &LogicalCreateIndex::BindTable(ClientContext &context, CreateIndexInfo &info_p) {
	auto &catalog = info_p.catalog;
	auto &schema = info_p.schema;
	auto &table_name = info_p.table;
	return Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table_name);
}

} // namespace duckdb
