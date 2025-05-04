#include "duckdb/parser/statement/refresh_materialized_view_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/parsed_data/refresh_materialized_view_info.hpp"

namespace duckdb {

unique_ptr<RefreshMaterializedViewStatement>
Transformer::TransformRefreshMaterializedView(duckdb_libpgquery::PGRefreshMaterializedViewStmt &stmt) {
	auto result = make_uniq<RefreshMaterializedViewStatement>();
	auto info = make_uniq<RefreshMaterializedViewInfo>();
	auto qname = TransformQualifiedName(*stmt.materializedView);

	info->type = CatalogType::MATERIALIZED_VIEW_ENTRY;
	info->table = qname.name;
	info->catalog = qname.catalog;
	info->schema = qname.schema;
	info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
