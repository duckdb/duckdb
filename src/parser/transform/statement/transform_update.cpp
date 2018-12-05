#include "parser/statement/update_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<UpdateStatement> Transformer::TransformUpdate(Node *node) {
	UpdateStmt *stmt = reinterpret_cast<UpdateStmt *>(node);
	assert(stmt);

	auto result = make_unique<UpdateStatement>();

	result->table = TransformRangeVar(stmt->relation);
	result->condition = TransformExpression(stmt->whereClause);

	List *root = stmt->targetList;
	for (auto cell = root->head; cell != NULL; cell = cell->next) {
		auto target = (ResTarget *)(cell->data.ptr_value);
		result->columns.push_back(target->name);
		result->expressions.push_back(TransformExpression(target->val));
	}
	return result;
}
