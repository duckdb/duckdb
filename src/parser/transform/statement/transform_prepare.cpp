#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PrepareStatement> Transformer::TransformPrepare(PGNode *node) {
	auto stmt = reinterpret_cast<PGPrepareStmt *>(node);
	assert(stmt);

	if (stmt->argtypes && stmt->argtypes->length > 0) {
		throw NotImplementedException("Prepared statement argument types are not supported, use CAST");
	}

	auto result = make_unique<PrepareStatement>();
	result->name = string(stmt->name);
	result->statement = TransformStatement(stmt->query);
	SetParamCount(0);

	return result;
}

unique_ptr<ExecuteStatement> Transformer::TransformExecute(PGNode *node) {
	auto stmt = reinterpret_cast<PGExecuteStmt *>(node);
	assert(stmt);

	auto result = make_unique<ExecuteStatement>();
	result->name = string(stmt->name);

	TransformExpressionList(stmt->params, result->values);
	for (auto &expr : result->values) {
		if (!expr->IsScalar()) {
			throw Exception("Only scalar parameters or NULL supported for EXECUTE");
		}
	}
	return result;
}

unique_ptr<DropStatement> Transformer::TransformDeallocate(PGNode *node) {
	auto stmt = reinterpret_cast<PGDeallocateStmt *>(node);
	assert(stmt);

	auto result = make_unique<DropStatement>();
	result->info->type = CatalogType::PREPARED_STATEMENT;
	result->info->name = string(stmt->name);
	return result;
}
