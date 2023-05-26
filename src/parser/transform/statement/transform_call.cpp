#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<CallStatement> Transformer::TransformCall(duckdb_libpgquery::PGCallStmt &stmt) {
	auto result = make_uniq<CallStatement>();
	result->function = TransformFuncCall(*PGPointerCast<duckdb_libpgquery::PGFuncCall>(stmt.func));
	return result;
}

} // namespace duckdb
