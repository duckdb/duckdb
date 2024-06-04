#include "duckdb/parser/statement/prepare_statement.hpp"

namespace duckdb {

PrepareStatement::PrepareStatement() : SQLStatement(StatementType::PREPARE_STATEMENT), statement(nullptr), name("") {
}

PrepareStatement::PrepareStatement(const PrepareStatement &other)
    : SQLStatement(other), statement(other.statement->Copy()), name(other.name) {
}

unique_ptr<SQLStatement> PrepareStatement::Copy() const {
	return unique_ptr<PrepareStatement>(new PrepareStatement(*this));
}

string PrepareStatement::ToString() const {
	string result = "";
	result += "PREPARE";
	result += " ";
	result += name;
	result += " ";
	result += "AS";
	result += " ";
	result += statement->ToString();
	// NOTE: We expect SQLStatement->ToString() to always end in a ';' ^
	return result;
}

} // namespace duckdb
