#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/refresh_matview_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/parsed_data/refresh_matview_info.hpp"

namespace duckdb {
BoundStatement Binder::Bind(RefreshMatViewStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	auto &properties = GetStatementProperties();
	auto &refresh_matview_info = stmt.info->Cast<RefreshMatViewInfo>();

	string matview_name = refresh_matview_info.table;
	auto entry =
	    Catalog::GetEntry<MatViewCatalogEntry>(context, refresh_matview_info.catalog, refresh_matview_info.schema,
	                                           refresh_matview_info.table, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		throw CatalogException("Materialized view %s does not exist!", matview_name);
	}
	auto column_names = entry->GetColumns().GetColumnNames();
	auto column_types = entry->GetColumns().GetColumnTypes();
	for (idx_t i = 0; i < column_names.size(); i++) {
		refresh_matview_info.columns.AddColumn(ColumnDefinition(column_names[i], column_types[i]));
	}

	auto bound_info = BindCreateTableInfo(std::move(make_uniq<RefreshMatViewInfo>(std::move(refresh_matview_info))));
	bound_info->query = entry->query->Copy(context);
	auto root = std::move(entry->query);
	auto &schema = bound_info->schema;
	auto refresh_matview = make_uniq<LogicalCreateTable>(schema, std::move(bound_info));
	if (root) {
		// CREATE MATERIALIZED VIEW AS SELECT
		properties.return_type = StatementReturnType::CHANGED_ROWS;
		refresh_matview->children.push_back(std::move(root));
	}

	result.plan = std::move(refresh_matview);
	properties.return_type = StatementReturnType::NOTHING;
	properties.allow_stream_result = false;
	return result;
}
} // namespace duckdb
