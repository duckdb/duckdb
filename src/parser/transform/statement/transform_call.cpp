#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

unique_ptr<CallStatement> Transformer::TransformCall(duckdb_libpgquery::PGCallStmt &stmt) {
	auto result = make_uniq<CallStatement>();
	result->function = TransformFuncCall(*PGPointerCast<duckdb_libpgquery::PGFuncCall>(stmt.func));
	return result;
}

} // namespace duckdb
