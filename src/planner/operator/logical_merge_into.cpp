#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

LogicalMergeInto::LogicalMergeInto(TableCatalogEntry &table)
    : LogicalOperator(LogicalOperatorType::LOGICAL_MERGE_INTO), table(table) {
}

LogicalMergeInto::LogicalMergeInto(ClientContext &context, const unique_ptr<CreateInfo> &table_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_MERGE_INTO),
      table(Catalog::GetEntry<TableCatalogEntry>(context, table_info->catalog, table_info->schema,
                                                 table_info->Cast<CreateTableInfo>().table)) {
	auto binder = Binder::CreateBinder(context);
	bound_constraints = binder->BindConstraints(table);
}

idx_t LogicalMergeInto::EstimateCardinality(ClientContext &context) {
	return 1;
}

vector<ColumnBinding> LogicalMergeInto::GetColumnBindings() {
	return {ColumnBinding(0, 0)};
}

void LogicalMergeInto::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

} // namespace duckdb
