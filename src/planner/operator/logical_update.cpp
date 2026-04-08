#include "duckdb/planner/operator/logical_update.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {
class ClientContext;

LogicalUpdate::LogicalUpdate(TableCatalogEntry &table)
    : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE), table(table), table_index(0), return_chunk(false) {
}

LogicalUpdate::LogicalUpdate(ClientContext &context, const unique_ptr<CreateInfo> &table_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE),
      table(Catalog::GetEntry<TableCatalogEntry>(context, table_info->catalog, table_info->schema,
                                                 table_info->Cast<CreateTableInfo>().table)) {
	auto binder = Binder::CreateBinder(context);
	bound_constraints = binder->BindConstraints(table);
}

idx_t LogicalUpdate::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

vector<ColumnBinding> LogicalUpdate::GetColumnBindings() {
	if (return_chunk) {
		return GenerateColumnBindings(table_index, table.GetTypes().size());
	}
	return {ColumnBinding(TableIndex(0), ProjectionIndex(0))};
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
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
