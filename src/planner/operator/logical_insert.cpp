#include "duckdb/planner/operator/logical_insert.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {
class ClientContext;

BoundOnConflictInfo::BoundOnConflictInfo()
    : action_type(OnConflictAction::THROW), excluded_table_index(0), update_is_del_and_insert(false) {
}

LogicalInsert::LogicalInsert(TableCatalogEntry &table, TableIndex table_index)
    : LogicalOperator(LogicalOperatorType::LOGICAL_INSERT), table(table), table_index(table_index),
      return_chunk(false) {
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

vector<TableIndex> LogicalInsert::GetTableIndex() const {
	return vector<TableIndex> {table_index};
}

vector<ColumnBinding> LogicalInsert::GetColumnBindings() {
	if (return_chunk) {
		return GenerateColumnBindings(table_index, table.GetTypes().size());
	}
	return {ColumnBinding(TableIndex(0), ProjectionIndex(0))};
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
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
