#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateTableAs(duckdb_libpgquery::PGCreateTableAsStmt &stmt) {
	if (stmt.relkind == duckdb_libpgquery::PG_OBJECT_MATVIEW) {
		throw NotImplementedException("Materialized view not implemented");
	}
	if (stmt.is_select_into || stmt.into->colNames || stmt.into->options) {
		throw NotImplementedException("Unimplemented features for CREATE TABLE as");
	}
	auto qname = TransformQualifiedName(*stmt.into->rel);
	if (stmt.query->type != duckdb_libpgquery::T_PGSelectStmt) {
		throw ParserException("CREATE TABLE AS requires a SELECT clause");
	}
	auto query = TransformSelectStmt(*stmt.query, false);

	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTableInfo>();
	info->catalog = qname.catalog;
	info->schema = qname.schema;
	info->table = qname.name;
	info->on_conflict = TransformOnConflict(stmt.onconflict);
	info->temporary =
	    stmt.into->rel->relpersistence == duckdb_libpgquery::PGPostgresRelPersistence::PG_RELPERSISTENCE_TEMP;
	info->query = std::move(query);
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
