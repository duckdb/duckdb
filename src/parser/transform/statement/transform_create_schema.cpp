#include "duckdb/parser/statement/create_schema_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<CreateSchemaStatement> Transformer::TransformCreateSchema(PGNode *node) {
	auto stmt = reinterpret_cast<PGCreateSchemaStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateSchemaStatement>();
	auto &info = *result->info.get();

	assert(stmt->schemaname);
	info.schema = stmt->schemaname;
	info.if_not_exists = stmt->if_not_exists;

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

	return result;
}
