#include "duckdb/parser/statement/load_statement.hpp"

namespace duckdb {

LoadStatement::LoadStatement() : SQLStatement(StatementType::LOAD_STATEMENT) {
}

unique_ptr<SQLStatement> LoadStatement::Copy() const {
	auto result = make_unique<LoadStatement>();
	result->info = info->Copy();
	return result;
}

} // namespace duckdb
