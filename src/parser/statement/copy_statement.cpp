#include "duckdb/parser/statement/copy_statement.hpp"

namespace duckdb {

CopyStatement::CopyStatement()
    : SQLStatement(StatementType::COPY_STATEMENT), info(make_uniq<CopyInfo>()), has_been_planned(false) {
}

CopyStatement::CopyStatement(const CopyStatement &other)
    : SQLStatement(other), info(other.info->Copy()), has_been_planned(other.has_been_planned) {
}

string CopyStatement::ToString() const {
	return info->ToString();
}

unique_ptr<SQLStatement> CopyStatement::Copy() const {
	return unique_ptr<CopyStatement>(new CopyStatement(*this));
}

} // namespace duckdb
