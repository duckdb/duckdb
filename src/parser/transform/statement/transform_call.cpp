#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<CallStatement> Transformer::TransformCall(PGNode *node) {
	auto stmt = reinterpret_cast<PGCallStmt *>(node);
	assert(stmt);

	auto result = make_unique<CallStatement>();
	result->function = TransformFuncCall((PGFuncCall *)stmt->func);
	return result;
}

} // namespace duckdb
