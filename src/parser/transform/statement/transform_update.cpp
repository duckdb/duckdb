#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<UpdateStatement> Transformer::TransformUpdate(PGNode *node) {
	auto stmt = reinterpret_cast<PGUpdateStmt *>(node);
	assert(stmt);

	auto result = make_unique<UpdateStatement>();

	result->table = TransformRangeVar(stmt->relation);
	if (stmt->fromClause) {
		result->from_table = TransformFrom(stmt->fromClause);
	}

	result->condition = TransformExpression(stmt->whereClause);

	auto root = stmt->targetList;
	for (auto cell = root->head; cell != NULL; cell = cell->next) {
		auto target = (PGResTarget *)(cell->data.ptr_value);
		result->columns.push_back(target->name);
		result->expressions.push_back(TransformExpression(target->val));
	}
	return result;
}

} // namespace duckdb
