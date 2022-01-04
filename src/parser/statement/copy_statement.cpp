#include "duckdb/parser/statement/copy_statement.hpp"

namespace duckdb {

CopyStatement::CopyStatement() : SQLStatement(StatementType::COPY_STATEMENT), info(make_unique<CopyInfo>()) {
}

unique_ptr<SQLStatement> CopyStatement::Copy() const {
	auto result = make_unique<CopyStatement>();
	result->info = info->Copy();
	if (select_statement) {
		result->select_statement = select_statement->Copy();
	}
	return result;
}

} // namespace duckdb
