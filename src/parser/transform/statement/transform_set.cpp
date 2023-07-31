#include "duckdb/parser/statement/set_statement.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

namespace {

SetScope ToSetScope(duckdb_libpgquery::VariableSetScope pg_scope) {
	switch (pg_scope) {
	case duckdb_libpgquery::VariableSetScope::VAR_SET_SCOPE_LOCAL:
		return SetScope::LOCAL;
	case duckdb_libpgquery::VariableSetScope::VAR_SET_SCOPE_SESSION:
		return SetScope::SESSION;
	case duckdb_libpgquery::VariableSetScope::VAR_SET_SCOPE_GLOBAL:
		return SetScope::GLOBAL;
	case duckdb_libpgquery::VariableSetScope::VAR_SET_SCOPE_DEFAULT:
		return SetScope::AUTOMATIC;
	default:
		throw InternalException("Unexpected pg_scope: %d", pg_scope);
	}
}

SetType ToSetType(duckdb_libpgquery::VariableSetKind pg_kind) {
	switch (pg_kind) {
	case duckdb_libpgquery::VariableSetKind::VAR_SET_VALUE:
		return SetType::SET;
	case duckdb_libpgquery::VariableSetKind::VAR_RESET:
		return SetType::RESET;
	default:
		throw NotImplementedException("Can only SET or RESET a variable");
	}
}

} // namespace

unique_ptr<SetStatement> Transformer::TransformSetVariable(duckdb_libpgquery::PGVariableSetStmt &stmt) {
	D_ASSERT(stmt.kind == duckdb_libpgquery::VariableSetKind::VAR_SET_VALUE);

	if (stmt.scope == duckdb_libpgquery::VariableSetScope::VAR_SET_SCOPE_LOCAL) {
		throw NotImplementedException("SET LOCAL is not implemented.");
	}

	auto name = std::string(stmt.name);
	D_ASSERT(!name.empty()); // parser protect us!
	if (stmt.args->length != 1) {
		throw ParserException("SET needs a single scalar value parameter");
	}
	D_ASSERT(stmt.args->head && stmt.args->head->data.ptr_value);

	auto ptr_value = stmt.args->head->data.ptr_value;

	Value value;
	if (auto const_val = PGCheckedCast<duckdb_libpgquery::PGAConst, duckdb_libpgquery::T_PGAConst>(ptr_value)) {
		value = TransformValue(const_val->val)->value;
	} else if (auto func_call =
	               PGCheckedCast<duckdb_libpgquery::PGFuncCall, duckdb_libpgquery::T_PGFuncCall>(ptr_value)) {
		auto func_expr = TransformFuncCall(*func_call);
		if (!ConstructConstantFromExpression(*func_expr, value)) {
			throw ParserException("Unsupported value type for setting");
		}
	} else {
		throw InternalException("Unexpected node type in SET statement");
	}

	return make_uniq<SetVariableStatement>(name, value, ToSetScope(stmt.scope));
}

unique_ptr<SetStatement> Transformer::TransformResetVariable(duckdb_libpgquery::PGVariableSetStmt &stmt) {
	D_ASSERT(stmt.kind == duckdb_libpgquery::VariableSetKind::VAR_RESET);

	if (stmt.scope == duckdb_libpgquery::VariableSetScope::VAR_SET_SCOPE_LOCAL) {
		throw NotImplementedException("RESET LOCAL is not implemented.");
	}

	auto name = std::string(stmt.name);
	D_ASSERT(!name.empty()); // parser protect us!

	return make_uniq<ResetVariableStatement>(name, ToSetScope(stmt.scope));
}

unique_ptr<SetStatement> Transformer::TransformSet(duckdb_libpgquery::PGVariableSetStmt &stmt) {
	D_ASSERT(stmt.type == duckdb_libpgquery::T_PGVariableSetStmt);

	SetType set_type = ToSetType(stmt.kind);

	switch (set_type) {
	case SetType::SET:
		return TransformSetVariable(stmt);
	case SetType::RESET:
		return TransformResetVariable(stmt);
	default:
		throw NotImplementedException("Type not implemented for SetType");
	}
}

} // namespace duckdb
