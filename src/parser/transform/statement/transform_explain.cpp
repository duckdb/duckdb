#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExplainStatement> Transformer::TransformExplain(PGNode *node) {
	PGExplainStmt *stmt = reinterpret_cast<PGExplainStmt *>(node);
	assert(stmt);
	return make_unique<ExplainStatement>(TransformStatement(stmt->query));
}
