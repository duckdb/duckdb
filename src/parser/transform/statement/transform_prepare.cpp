#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

unique_ptr<PrepareStatement> Transformer::TransformPrepare(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGPrepareStmt *>(node);
	D_ASSERT(stmt);

	if (stmt->argtypes && stmt->argtypes->length > 0) {
		throw NotImplementedException("Prepared statement argument types are not supported, use CAST");
	}

	auto result = make_uniq<PrepareStatement>();
	result->name = string(stmt->name);
	result->statement = TransformStatement(stmt->query);
	SetParamCount(0);

	return result;
}

static string NotAcceptedExpressionException() {
	return "Only scalar parameters, named parameters or NULL supported for EXECUTE";
}

void VerifyNamedParameterExpression(ComparisonExpression &expr) {
	auto &name = expr.left;
	auto &value = expr.right;
	if (name->type != ExpressionType::COLUMN_REF) {
		throw InvalidInputException("Expected a parameter name, found expression of type '%s' instead",
		                            ExpressionTypeToString(name->type));
	}
	if (!value->IsScalar()) {
		throw InvalidInputException(NotAcceptedExpressionException());
	}
}

unique_ptr<ExecuteStatement> Transformer::TransformExecute(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGExecuteStmt *>(node);
	D_ASSERT(stmt);

	auto result = make_uniq<ExecuteStatement>();
	result->name = string(stmt->name);

	vector<unique_ptr<ParsedExpression>> intermediate_values;
	if (stmt->params) {
		TransformExpressionList(*stmt->params, intermediate_values);
	}
	for (auto &expr : intermediate_values) {
		if (expr->type == ExpressionType::COMPARE_EQUAL) {
			auto &name_and_value = expr->Cast<ComparisonExpression>();
			VerifyNamedParameterExpression(name_and_value);
			auto &name = name_and_value.left->Cast<ColumnRefExpression>();
			result->named_values[name.GetColumnName()] = std::move(name_and_value.right);
		} else if (expr->IsScalar()) {
			result->values.push_back(std::move(expr));
		} else {
			throw InvalidInputException(NotAcceptedExpressionException());
		}
	}
	intermediate_values.clear();
	if (!result->named_values.empty() && !result->values.empty()) {
		throw NotImplementedException("Mixing named parameters and positional parameters is not supported yet");
	}
	return result;
}

unique_ptr<DropStatement> Transformer::TransformDeallocate(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGDeallocateStmt *>(node);
	D_ASSERT(stmt);
	if (!stmt->name) {
		throw ParserException("DEALLOCATE requires a name");
	}

	auto result = make_uniq<DropStatement>();
	result->info->type = CatalogType::PREPARED_STATEMENT;
	result->info->name = string(stmt->name);
	return result;
}

} // namespace duckdb
