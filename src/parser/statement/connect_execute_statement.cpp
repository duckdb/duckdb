#include "duckdb/parser/statement/connect_execute_statement.hpp"

namespace duckdb {

ConnectExecuteStatement::ConnectExecuteStatement() : SQLStatement(StatementType::CONNECT_EXECUTE_STATEMENT) {
}

ConnectExecuteStatement::ConnectExecuteStatement(string target_p)
    : SQLStatement(StatementType::CONNECT_EXECUTE_STATEMENT), target(std::move(target_p)) {
}

ConnectExecuteStatement::ConnectExecuteStatement(const ConnectExecuteStatement &other)
    : SQLStatement(other), target(other.target), target_is_local(other.target_is_local) {
}

unique_ptr<SQLStatement> ConnectExecuteStatement::Copy() const {
	return unique_ptr<ConnectExecuteStatement>(new ConnectExecuteStatement(*this));
}

string ConnectExecuteStatement::ToString() const {
	return "CONNECT " + (target_is_local ? string("LOCAL") : target) + " EXECUTE";
}

} // namespace duckdb
