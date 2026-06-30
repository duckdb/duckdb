#include "duckdb/parser/statement/connect_statement.hpp"

namespace duckdb {

ConnectStatement::ConnectStatement() : SQLStatement(StatementType::CONNECT_STATEMENT) {
}

ConnectStatement::ConnectStatement(const ConnectStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> ConnectStatement::Copy() const {
	return unique_ptr<ConnectStatement>(new ConnectStatement(*this));
}

string ConnectStatement::ToString() const {
	return info->ToString();
}

} // namespace duckdb
