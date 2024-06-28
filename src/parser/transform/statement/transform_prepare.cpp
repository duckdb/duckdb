#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

unique_ptr<PrepareStatement> Transformer::TransformPrepare(duckdb_libpgquery::PGPrepareStmt &stmt) {
	if (stmt.argtypes && stmt.argtypes->length > 0) {
		throw NotImplementedException("Prepared statement argument types are not supported, use CAST");
	}

	auto result = make_uniq<PrepareStatement>();
	result->name = string(stmt.name);
	result->statement = TransformStatement(*stmt.query);
	SetParamCount(0);

	return result;
}

static string NotAcceptedExpressionException() {
	return "Only scalar parameters, named parameters or NULL supported for EXECUTE";
}

unique_ptr<ExecuteStatement> Transformer::TransformExecute(duckdb_libpgquery::PGExecuteStmt &stmt) {
	auto result = make_uniq<ExecuteStatement>();
	result->name = string(stmt.name);

	vector<unique_ptr<ParsedExpression>> intermediate_values;
	if (stmt.params) {
		TransformExpressionList(*stmt.params, intermediate_values);
	}

	idx_t param_idx = 0;
	for (idx_t i = 0; i < intermediate_values.size(); i++) {
		auto &expr = intermediate_values[i];
		if (!expr->IsScalar()) {
			throw InvalidInputException(NotAcceptedExpressionException());
		}
		if (!expr->alias.empty() && param_idx != 0) {
			// Found unnamed parameters mixed with named parameters
			throw NotImplementedException("Mixing named parameters and positional parameters is not supported yet");
		}
		auto param_name = expr->alias;
		if (expr->alias.empty()) {
			param_name = std::to_string(param_idx + 1);
			if (param_idx != i) {
				throw NotImplementedException("Mixing named parameters and positional parameters is not supported yet");
			}
			param_idx++;
		}
		expr->alias.clear();
		result->named_values[param_name] = std::move(expr);
	}
	intermediate_values.clear();
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
