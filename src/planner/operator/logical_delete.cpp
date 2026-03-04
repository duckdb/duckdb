#include "duckdb/planner/operator/logical_delete.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

LogicalDelete::LogicalDelete(TableCatalogEntry &table, idx_t table_index)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DELETE), table(table), table_index(table_index),
      return_chunk(false) {
}

LogicalDelete::LogicalDelete(ClientContext &context, const unique_ptr<CreateInfo> &table_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DELETE),
      table(Catalog::GetEntry<TableCatalogEntry>(context, table_info->catalog, table_info->schema,
                                                 table_info->Cast<CreateTableInfo>().table)) {
	auto binder = Binder::CreateBinder(context);
	bound_constraints = binder->BindConstraints(table);
}

idx_t LogicalDelete::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

vector<idx_t> LogicalDelete::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

vector<ColumnBinding> LogicalDelete::GetColumnBindings() {
	if (return_chunk) {
		// Include table columns + virtual columns (e.g., rowid)
		auto virtual_columns = table.GetVirtualColumns();
		return GenerateColumnBindings(table_index, table.GetTypes().size() + virtual_columns.size());
	}
	return {ColumnBinding(0, 0)};
}

void LogicalDelete::ResolveTypes() {
	if (return_chunk) {
		types = table.GetTypes();
		// Add virtual columns (e.g., rowid)
		auto virtual_columns = table.GetVirtualColumns();
		for (auto &entry : virtual_columns) {
			types.push_back(entry.second.type);
		}
	} else {
		types.emplace_back(LogicalType::BIGINT);
	}
}

string LogicalDelete::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
