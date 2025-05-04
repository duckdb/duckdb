#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/refresh_materialized_view_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/parsed_data/refresh_materialized_view_info.hpp"

namespace duckdb {
BoundStatement Binder::Bind(RefreshMaterializedViewStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	auto &properties = GetStatementProperties();
	auto &refresh_materialized_view_info = stmt.info->Cast<RefreshMaterializedViewInfo>();

	string materialized_view_name = refresh_materialized_view_info.table;
	auto entry = Catalog::GetEntry<MaterializedViewCatalogEntry>(
	    context, refresh_materialized_view_info.catalog, refresh_materialized_view_info.schema,
	    refresh_materialized_view_info.table, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		throw CatalogException("Materialized view %s does not exist!", materialized_view_name);
	}
	auto column_names = entry->GetColumns().GetColumnNames();
	auto column_types = entry->GetColumns().GetColumnTypes();
	for (idx_t i = 0; i < column_names.size(); i++) {
		refresh_materialized_view_info.columns.AddColumn(ColumnDefinition(column_names[i], column_types[i]));
	}

	auto bound_info = BindCreateTableInfo(
	    std::move(make_uniq<RefreshMaterializedViewInfo>(std::move(refresh_materialized_view_info))));
	bound_info->query = entry->query->Copy(context);
	auto root = std::move(entry->query);
	auto &schema = bound_info->schema;
	auto refresh_materialized_view = make_uniq<LogicalCreateTable>(schema, std::move(bound_info));
	if (root) {
		// CREATE MATERIALIZED VIEW AS SELECT
		properties.return_type = StatementReturnType::CHANGED_ROWS;
		refresh_materialized_view->children.push_back(std::move(root));
	}

	result.plan = std::move(refresh_materialized_view);
	properties.return_type = StatementReturnType::NOTHING;
	properties.allow_stream_result = false;
	return result;
}
} // namespace duckdb
