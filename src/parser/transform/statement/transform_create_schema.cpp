#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<CreateStatement> Transformer::TransformCreateSchema(PGNode *node) {
	auto stmt = reinterpret_cast<PGCreateSchemaStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateSchemaInfo>();

	assert(stmt->schemaname);
	info->schema = stmt->schemaname;
	info->on_conflict = stmt->if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;

	if (stmt->schemaElts) {
		// schema elements
		for (auto cell = stmt->schemaElts->head; cell != nullptr; cell = cell->next) {
			auto node = reinterpret_cast<PGNode *>(cell->data.ptr_value);
			switch (node->type) {
			case T_PGCreateStmt:
			case T_PGViewStmt:
			default:
				throw NotImplementedException("Schema element not supported yet!");
			}
		}
	}
	result->info = move(info);
	return result;
}

} // namespace duckdb
