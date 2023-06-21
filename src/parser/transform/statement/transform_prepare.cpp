#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<PrepareStatement> Transformer::TransformPrepare(duckdb_libpgquery::PGPrepareStmt &stmt) {
	if (stmt.argtypes && stmt.argtypes->length > 0) {
		throw NotImplementedException("Prepared statement argument types are not supported, use CAST");
	}

	auto result = make_uniq<PrepareStatement>();
	result->name = string(stmt.name);
	result->statement = TransformStatement(*stmt.query);
	if (!result->statement->named_param_map.empty()) {
		throw NotImplementedException("Named parameters are not supported in this client yet");
	}
	SetParamCount(0);

	return result;
}

unique_ptr<ExecuteStatement> Transformer::TransformExecute(duckdb_libpgquery::PGExecuteStmt &stmt) {
	auto result = make_uniq<ExecuteStatement>();
	result->name = string(stmt.name);

	if (stmt.params) {
		TransformExpressionList(*stmt.params, result->values);
	}
	for (auto &expr : result->values) {
		if (!expr->IsScalar()) {
			throw Exception("Only scalar parameters or NULL supported for EXECUTE");
		}
	}
	return result;
}

unique_ptr<DropStatement> Transformer::TransformDeallocate(duckdb_libpgquery::PGDeallocateStmt &stmt) {
	if (!stmt.name) {
		throw ParserException("DEALLOCATE requires a name");
	}

	auto result = make_uniq<DropStatement>();
	result->info->type = CatalogType::PREPARED_STATEMENT;
	result->info->name = string(stmt.name);
	return result;
}

} // namespace duckdb
