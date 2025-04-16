#include "duckdb/parser/statement/refresh_matview_statement.hpp"

namespace duckdb {

RefreshMatViewStatement::RefreshMatViewStatement() : SQLStatement(StatementType::REFRESH_MATVIEW_STATEMENT) {
}

RefreshMatViewStatement::RefreshMatViewStatement(const RefreshMatViewStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> RefreshMatViewStatement::Copy() const {
	return unique_ptr<RefreshMatViewStatement>(new RefreshMatViewStatement(*this));
}

string RefreshMatViewStatement::ToString() const {
	return info->ToString();
}

} // namespace duckdb
