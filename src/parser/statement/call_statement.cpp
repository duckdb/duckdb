#include "duckdb/parser/statement/call_statement.hpp"

namespace duckdb {

CallStatement::CallStatement() : SQLStatement(StatementType::CALL_STATEMENT) {
}

CallStatement::CallStatement(const CallStatement &other) : SQLStatement(other), function(other.function->Copy()) {
}

unique_ptr<SQLStatement> CallStatement::Copy() const {
	return unique_ptr<CallStatement>(new CallStatement(*this));
}

string CallStatement::ToString() const {
	string result = "";
	result += "CALL";
	result += " " + function->ToString();
	result += ";";
	return result;
}

} // namespace duckdb
