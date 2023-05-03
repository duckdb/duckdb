#include "duckdb/parser/statement/explain_statement.hpp"

namespace duckdb {

ExplainStatement::ExplainStatement(unique_ptr<SQLStatement> stmt, ExplainType explain_type)
    : SQLStatement(StatementType::EXPLAIN_STATEMENT), stmt(std::move(stmt)), explain_type(explain_type) {
}

ExplainStatement::ExplainStatement(const ExplainStatement &other)
    : SQLStatement(other), stmt(other.stmt->Copy()), explain_type(other.explain_type) {
}

unique_ptr<SQLStatement> ExplainStatement::Copy() const {
	return unique_ptr<ExplainStatement>(new ExplainStatement(*this));
}

} // namespace duckdb
