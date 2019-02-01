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

// unique_ptr<UpdateStatement> Transformer::TransformExecute(Node *node) {
//	ExecuteStmt *stmt = reinterpret_cast<ExecuteStmt *>(node);
//	assert(stmt);
//
//	throw NotImplementedException("");
//}
//
// unique_ptr<UpdateStatement> Transformer::TransformDeallocate(Node *node) {
//	DeallocateStmt *stmt = reinterpret_cast<DeallocateStmt *>(node);
//	assert(stmt);
//
//	throw NotImplementedException("");
//}
