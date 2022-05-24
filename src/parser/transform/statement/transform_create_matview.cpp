#include "duckdb/parser/parsed_data/create_matview_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateMatView(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateTableAsStmt *>(node);
	D_ASSERT(stmt);
	if (stmt->relkind != duckdb_libpgquery::PG_OBJECT_MATVIEW) {
		throw NotImplementedException("TransformCreateMatView only support MATERIALIZED VIEW!");
	}
	if (stmt->is_select_into || stmt->into->colNames || stmt->into->options) {
		throw NotImplementedException("Unimplemented features for CREATE MATERIALIZED VIEW");
	}
	auto qname = TransformQualifiedName(stmt->into->rel);
	if (stmt->query->type != duckdb_libpgquery::T_PGSelectStmt) {
		throw ParserException("CREATE MATERIALIZED VIEW requires a SELECT clause");
	}
	auto query = TransformSelect(stmt->query, false);

	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateMatViewInfo>();
	info->schema = qname.schema;
	info->table = qname.name;
	info->on_conflict = TransformOnConflict(stmt->onconflict);
	info->temporary =
	    stmt->into->rel->relpersistence == duckdb_libpgquery::PGPostgresRelPersistence::PG_RELPERSISTENCE_TEMP;
	info->query = move(query);
	result->info = move(info);
	return result;
}

} // namespace duckdb
