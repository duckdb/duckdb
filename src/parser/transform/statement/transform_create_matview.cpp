#include "duckdb/parser/parsed_data/create_matview_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateMatView(duckdb_libpgquery::PGCreateMatViewStmt &stmt) {
	if (stmt.relkind != duckdb_libpgquery::PG_OBJECT_MATVIEW) {
		throw NotImplementedException("TransformCreateMatView only support MATERIALIZED VIEW!");
	}
	if (stmt.is_select_into || stmt.into->colNames || stmt.into->options) {
		throw NotImplementedException("Unimplemented features for CREATE MATERIALIZED VIEW");
	}
	if (stmt.query->type != duckdb_libpgquery::T_PGSelectStmt) {
		throw ParserException("CREATE MATERIALIZED VIEW requires a SELECT clause");
	}
	auto qname = TransformQualifiedName(*stmt.into->rel);
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateMatViewInfo>();
	auto query = TransformSelectStmt(*stmt.query, false);

	info->catalog = qname.catalog;
	info->schema = qname.schema;
	info->table = qname.name;
	info->query = std::move(query);
	info->on_conflict = TransformOnConflict(stmt.onconflict);

	result->info = std::move(info);
	return result;
}

} // namespace duckdb
