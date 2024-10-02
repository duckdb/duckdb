#include "duckdb/planner/operator/logical_create_table.hpp"

#include "duckdb/planner/binder.hpp"

namespace duckdb {

LogicalCreateTable::LogicalCreateTable(SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_TABLE), schema(schema), info(std::move(info)) {
}

LogicalCreateTable::LogicalCreateTable(ClientContext &context, unique_ptr<CreateInfo> unbound_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_TABLE),
      schema(Catalog::GetSchema(context, unbound_info->catalog, unbound_info->schema)) {
	D_ASSERT(unbound_info->type == CatalogType::TABLE_ENTRY);
	auto binder = Binder::CreateBinder(context);
	info = binder->BindCreateTableInfo(unique_ptr_cast<CreateInfo, CreateTableInfo>(std::move(unbound_info)));
}

idx_t LogicalCreateTable::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalCreateTable::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

} // namespace duckdb
