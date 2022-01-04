#include "duckdb/parser/statement/create_statement.hpp"

namespace duckdb {

CreateStatement::CreateStatement() : SQLStatement(StatementType::CREATE_STATEMENT) {
}

unique_ptr<SQLStatement> CreateStatement::Copy() const {
	auto result = make_unique<CreateStatement>();
	result->info = info->Copy();
	return result;
}

} // namespace duckdb
