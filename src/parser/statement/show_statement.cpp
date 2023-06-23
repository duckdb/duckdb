#include "duckdb/parser/statement/show_statement.hpp"

namespace duckdb {

ShowStatement::ShowStatement() : SQLStatement(StatementType::SHOW_STATEMENT), info(make_uniq<ShowSelectInfo>()) {
}

ShowStatement::ShowStatement(const ShowStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> ShowStatement::Copy() const {
	return unique_ptr<ShowStatement>(new ShowStatement(*this));
}

} // namespace duckdb
