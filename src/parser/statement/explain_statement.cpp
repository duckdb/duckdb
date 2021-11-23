#include "duckdb/parser/statement/explain_statement.hpp"

namespace duckdb {

ExplainStatement::ExplainStatement(unique_ptr<SQLStatement> stmt, ExplainType explain_type)
    : SQLStatement(StatementType::EXPLAIN_STATEMENT), stmt(move(stmt)), explain_type(explain_type) {
}

unique_ptr<SQLStatement> ExplainStatement::Copy() const {
	auto result = make_unique<ExplainStatement>(stmt->Copy(), explain_type);
	return move(result);
}

} // namespace duckdb
