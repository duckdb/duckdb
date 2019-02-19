#include "parser/statement/create_table_statement.hpp"
#include "parser/tableref/basetableref.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<CreateTableStatement> Transformer::TransformCreateTableAs(postgres::Node *node) {
	auto stmt = reinterpret_cast<CreateTableAsStmt *>(node);
	assert(stmt);
	if (stmt->relkind == OBJECT_MATVIEW) {
		throw NotImplementedException("Materialized view not implemented");
	}
	if (stmt->is_select_into || stmt->into->colNames || stmt->into->options) {
		throw NotImplementedException("Unimplemented features for CREATE TABLE as");
	}
	auto tableref = TransformRangeVar(stmt->into->rel);
	auto query = TransformSelect(stmt->query);
	auto &basetable = (BaseTableRef &)*tableref;
	auto result = make_unique<CreateTableStatement>();
	result->info->schema = basetable.schema_name;
	result->info->table = basetable.table_name;
	result->info->if_not_exists = stmt->if_not_exists;
	result->query = move(query);
	return result;
}
