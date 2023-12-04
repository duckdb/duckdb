#include "duckdb/parser/statement/execute_statement.hpp"

namespace duckdb {

ExecuteStatement::ExecuteStatement() : SQLStatement(StatementType::EXECUTE_STATEMENT) {
}

ExecuteStatement::ExecuteStatement(const ExecuteStatement &other) : SQLStatement(other), name(other.name) {
	for (const auto &item : other.named_values) {
		named_values.emplace(std::make_pair(item.first, item.second->Copy()));
	}
}

unique_ptr<SQLStatement> ExecuteStatement::Copy() const {
	return unique_ptr<ExecuteStatement>(new ExecuteStatement(*this));
}

} // namespace duckdb
