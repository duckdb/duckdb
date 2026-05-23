#include "duckdb/parser/statement/disconnect_statement.hpp"

namespace duckdb {

DisconnectStatement::DisconnectStatement() : SQLStatement(StatementType::DISCONNECT_STATEMENT) {
}

DisconnectStatement::DisconnectStatement(const DisconnectStatement &other)
    : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> DisconnectStatement::Copy() const {
	return unique_ptr<DisconnectStatement>(new DisconnectStatement(*this));
}

string DisconnectStatement::ToString() const {
	return info->ToString();
}

} // namespace duckdb
