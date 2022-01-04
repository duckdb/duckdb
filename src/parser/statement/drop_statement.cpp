#include "duckdb/parser/statement/drop_statement.hpp"

namespace duckdb {

DropStatement::DropStatement() : SQLStatement(StatementType::DROP_STATEMENT), info(make_unique<DropInfo>()) {
}

unique_ptr<SQLStatement> DropStatement::Copy() const {
	auto result = make_unique<DropStatement>();
	result->info = info->Copy();
	return result;
}

} // namespace duckdb
