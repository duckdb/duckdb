#include "parser/statement/create_schema_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<CreateSchemaStatement> Transformer::TransformCreateSchema(Node *node) {
	auto stmt = reinterpret_cast<CreateSchemaStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateSchemaStatement>();
	auto &info = *result->info.get();

	assert(stmt->schemaname);
	info.schema = stmt->schemaname;
	info.if_not_exists = stmt->if_not_exists;

	if (stmt->authrole) {
		auto authrole = reinterpret_cast<Node *>(stmt->authrole);
		switch (authrole->type) {
		case T_RoleSpec:
		default:
			throw NotImplementedException("Authrole not implemented yet!");
		}
	}

	if (stmt->schemaElts) {
		// schema elements
		for (auto cell = stmt->schemaElts->head; cell != nullptr; cell = cell->next) {
			auto node = reinterpret_cast<Node *>(cell->data.ptr_value);
			switch (node->type) {
			case T_CreateStmt:
			case T_ViewStmt:
			default:
				throw NotImplementedException("Schema element not supported yet!");
			}
		}
	}

	return result;
}
