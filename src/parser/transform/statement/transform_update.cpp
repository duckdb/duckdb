#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<UpdateStatement> Transformer::TransformUpdate(postgres::Node *node) {
	auto stmt = reinterpret_cast<postgres::UpdateStmt *>(node);
	assert(stmt);

	auto result = make_unique<UpdateStatement>();

	result->table = TransformRangeVar(stmt->relation);
	result->condition = TransformExpression(stmt->whereClause);

	auto root = stmt->targetList;
	for (auto cell = root->head; cell != NULL; cell = cell->next) {
		auto target = (postgres::ResTarget *)(cell->data.ptr_value);
		result->columns.push_back(target->name);
		result->expressions.push_back(TransformExpression(target->val));
	}
	return result;
}
