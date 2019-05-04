#include "parser/statement/deallocate_statement.hpp"
#include "parser/statement/execute_statement.hpp"
#include "parser/statement/prepare_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<PrepareStatement> Transformer::TransformPrepare(Node *node) {
	PrepareStmt *stmt = reinterpret_cast<PrepareStmt *>(node);
	assert(stmt);

	if (stmt->argtypes && stmt->argtypes->length > 0) {
		throw NotImplementedException("Prepared statement argument types are not supported, use CAST");
	}

	auto result = make_unique<PrepareStatement>();
	result->name = string(stmt->name);
	result->statement = TransformStatement(stmt->query);

	return result;
}

unique_ptr<ExecuteStatement> Transformer::TransformExecute(Node *node) {
	ExecuteStmt *stmt = reinterpret_cast<ExecuteStmt *>(node);
	assert(stmt);

	auto result = make_unique<ExecuteStatement>();
	result->name = string(stmt->name);

	TransformExpressionList(stmt->params, result->values);
	for (auto &expr : result->values) {
		if (expr->GetExpressionType() != ExpressionType::VALUE_CONSTANT &&
		    expr->GetExpressionType() != ExpressionType::VALUE_NULL) {
			throw Exception("Only scalar parameters or NULL supported for EXECUTE");
		}
	}

	return result;
}

unique_ptr<DeallocateStatement> Transformer::TransformDeallocate(Node *node) {
	DeallocateStmt *stmt = reinterpret_cast<DeallocateStmt *>(node);
	assert(stmt);

	// TODO empty name means all are removed
	auto result = make_unique<DeallocateStatement>(string(stmt->name));

	return result;
}
