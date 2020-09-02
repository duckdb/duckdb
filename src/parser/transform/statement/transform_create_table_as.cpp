#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<CreateStatement> Transformer::TransformCreateTableAs(PGNode *node) {
	auto stmt = reinterpret_cast<PGCreateTableAsStmt *>(node);
	assert(stmt);
	if (stmt->relkind == PG_OBJECT_MATVIEW) {
		throw NotImplementedException("Materialized view not implemented");
	}
	if (stmt->is_select_into || stmt->into->colNames || stmt->into->options) {
		throw NotImplementedException("Unimplemented features for CREATE TABLE as");
	}
	auto qname = TransformQualifiedName(stmt->into->rel);
	auto query = TransformSelect(stmt->query);

	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateTableInfo>();
	info->schema = qname.schema;
	info->table = qname.name;
	info->on_conflict = stmt->if_not_exists ? OnCreateConflict::IGNORE : OnCreateConflict::ERROR;
	info->query = move(query);
	result->info = move(info);
	return result;
}

} // namespace duckdb
