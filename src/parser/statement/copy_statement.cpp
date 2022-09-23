#include "duckdb/parser/statement/copy_statement.hpp"

namespace duckdb {

CopyStatement::CopyStatement() : SQLStatement(StatementType::COPY_STATEMENT), info(make_unique<CopyInfo>()) {
}

CopyStatement::CopyStatement(const CopyStatement &other) : SQLStatement(other), info(other.info->Copy()) {
	if (other.select_statement) {
		select_statement = other.select_statement->Copy();
	}
}

unique_ptr<SQLStatement> CopyStatement::Copy() const {
	return unique_ptr<CopyStatement>(new CopyStatement(*this));
}

} // namespace duckdb
