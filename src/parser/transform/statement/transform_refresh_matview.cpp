#include "duckdb/parser/statement/refresh_matview_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/parsed_data/refresh_matview_info.hpp"

namespace duckdb {

unique_ptr<RefreshMatViewStatement> Transformer::TransformRefreshMatView(duckdb_libpgquery::PGRefreshMatViewStmt &stmt) {
	auto result = make_uniq<RefreshMatViewStatement>();
	auto info = make_uniq<RefreshMatViewInfo>();
	auto qname = TransformQualifiedName(*stmt.matView);

	info->type = CatalogType::MATVIEW_ENTRY;
	info->table = qname.name;
	info->catalog = qname.catalog;
	info->schema = qname.schema;
	// TODO: disable later
	info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
