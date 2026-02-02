#include "duckdb/planner/operator/logical_merge_into.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"

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
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

vector<idx_t> LogicalMergeInto::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

vector<ColumnBinding> LogicalMergeInto::GetColumnBindings() {
	if (return_chunk) {
		return GenerateColumnBindings(table_index, table.GetTypes().size() + 1);
	}
	return {ColumnBinding(0, 0)};
}

void LogicalMergeInto::ResolveTypes() {
	if (return_chunk) {
		types = table.GetTypes();
		types.push_back(LogicalType::VARCHAR);
	} else {
		types.emplace_back(LogicalType::BIGINT);
	}
}

} // namespace duckdb
