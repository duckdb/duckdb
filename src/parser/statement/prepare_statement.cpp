#include "duckdb/parser/statement/prepare_statement.hpp"

#include "duckdb/common/sql_identifier.hpp"

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
	SQLRenderContext context;
	return ToString(context);
}

string PrepareStatement::ToString(SQLRenderContext &context) const {
	auto statement_string = statement->ToString(context);
	if (context.probe_only) {
		return string();
	}
	string result = "";
	result += "PREPARE";
	result += " ";
	result += SQLIdentifier(name);
	result += " ";
	result += "AS";
	result += " ";
	result += statement_string;
	// NOTE: We expect SQLStatement->ToString() to always end in a ';' ^
	return result;
}

} // namespace duckdb
