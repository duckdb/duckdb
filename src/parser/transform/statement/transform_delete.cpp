#include "parser/statement/delete_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<DeleteStatement> Transformer::TransformDelete(Node *node) {
	DeleteStmt *stmt = reinterpret_cast<DeleteStmt *>(node);
	assert(stmt);
	auto result = make_unique<DeleteStatement>();

	result->condition = TransformExpression(stmt->whereClause);
	result->table = TransformRangeVar(stmt->relation);
	if (result->table->type != TableReferenceType::BASE_TABLE) {
		throw Exception("Can only delete from base tables!");
	}
	return result;
}
