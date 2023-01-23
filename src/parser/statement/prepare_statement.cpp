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

bool PrepareStatement::Equals(const SQLStatement *other_p) const {
	if (other->type != type) {
		return false;
	}
	auto &other = (const PrepareStatement &)*other_p;
	if (name != other.name) {
		return false;
	}
	return statement->Equals(other.statement.get());
}

} // namespace duckdb
