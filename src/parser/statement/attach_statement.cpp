#include "duckdb/parser/statement/attach_statement.hpp"

namespace duckdb {

AttachStatement::AttachStatement() : SQLStatement(StatementType::ATTACH_STATEMENT) {
}

AttachStatement::AttachStatement(const AttachStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> AttachStatement::Copy() const {
	return unique_ptr<AttachStatement>(new AttachStatement(*this));
}

string AttachStatement::ToString() const {
	return info->ToString();
}

} // namespace duckdb
