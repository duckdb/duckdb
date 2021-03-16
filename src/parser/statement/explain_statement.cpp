#include "duckdb/parser/statement/explain_statement.hpp"

namespace duckdb {

ExplainStatement::ExplainStatement(unique_ptr<SQLStatement> stmt)
    : SQLStatement(StatementType::EXPLAIN_STATEMENT), stmt(move(stmt)) {
}

unique_ptr<SQLStatement> ExplainStatement::Copy() const {
	auto result = make_unique<ExplainStatement>(stmt->Copy());
	return move(result);
}

} // namespace duckdb
