#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<CreateStatement> Transformer::TransformCreateTableAs(PGNode *node) {
	auto stmt = reinterpret_cast<PGCreateTableAsStmt *>(node);
	assert(stmt);
	if (stmt->relkind == PG_OBJECT_MATVIEW) {
		throw NotImplementedException("Materialized view not implemented");
	}
	if (stmt->is_select_into || stmt->into->colNames || stmt->into->options) {
		throw NotImplementedException("Unimplemented features for CREATE TABLE as");
	}
	auto tableref = TransformRangeVar(stmt->into->rel);
	auto query = TransformSelect(stmt->query);
	auto &basetable = (BaseTableRef &)*tableref;

	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateTableInfo>();
	info->schema = basetable.schema_name;
	info->table = basetable.table_name;
	info->on_conflict = stmt->if_not_exists ? OnCreateConflict::IGNORE : OnCreateConflict::ERROR;
	info->query = move(query);
	result->info = move(info);
	return result;
}
