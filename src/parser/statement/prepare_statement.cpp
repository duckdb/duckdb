#include "duckdb/parser/statement/prepare_statement.hpp"

namespace duckdb {

PrepareStatement::PrepareStatement() : SQLStatement(StatementType::PREPARE_STATEMENT), statement(nullptr), name("") {
}

unique_ptr<SQLStatement> PrepareStatement::Copy() const {
	auto result = make_unique<PrepareStatement>();
	result->statement = statement->Copy();
	result->name = name;
	return result;
}

} // namespace duckdb
