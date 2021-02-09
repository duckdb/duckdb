#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

unique_ptr<SetStatement> Transformer::TransformSet(PGNode *node) {
	D_ASSERT(node->type == duckdb_libpgquery::T_PGVariableSetStmt);
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGVariableSetStmt *>(node);

	if (stmt->kind != VariableSetKind::VAR_SET_VALUE) {
		throw ParserException("Can only SET a variable to a value");
	}
	auto name = std::string(stmt->name);
	D_ASSERT(!name.empty()); // parser protect us!
	if (stmt->args->length != 1) {
		throw ParserException("SET needs a single scalar value parameter");
	}
	D_ASSERT(stmt->args->head && stmt->args->head->data.ptr_value);
	D_ASSERT(((PGNode *)stmt->args->head->data.ptr_value)->type == T_PGAConst);

	auto value = TransformValue(((PGAConst *)stmt->args->head->data.ptr_value)->val)->value;

	return make_unique<SetStatement>(name, value);
}

} // namespace duckdb
