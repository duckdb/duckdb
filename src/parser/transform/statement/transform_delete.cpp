#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<DeleteStatement> Transformer::TransformDelete(PGNode *node) {
	auto stmt = reinterpret_cast<PGDeleteStmt *>(node);
	assert(stmt);
	auto result = make_unique<DeleteStatement>();

	result->condition = TransformExpression(stmt->whereClause);
	result->table = TransformRangeVar(stmt->relation);
	if (result->table->type != TableReferenceType::BASE_TABLE) {
		throw Exception("Can only delete from base tables!");
	}
	return result;
}

} // namespace duckdb
