#include "duckdb/planner/operator/logical_insert.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

LogicalInsert::LogicalInsert(TableCatalogEntry &table, idx_t table_index)
    : LogicalOperator(LogicalOperatorType::LOGICAL_INSERT), table(table), table_index(table_index), return_chunk(false),
      action_type(OnConflictAction::THROW), update_is_del_and_insert(false) {
}

LogicalInsert::LogicalInsert(ClientContext &context, const unique_ptr<CreateInfo> table_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_INSERT),
      table(Catalog::GetEntry<TableCatalogEntry>(context, table_info->catalog, table_info->schema,
                                                 table_info->Cast<CreateTableInfo>().table)) {
	auto binder = Binder::CreateBinder(context);
	bound_constraints = binder->BindConstraints(table);
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
