#include "duckdb/parser/statement/show_statement.hpp"

namespace duckdb {

ShowStatement::ShowStatement() : SQLStatement(StatementType::SHOW_STATEMENT), info(make_unique<ShowSelectInfo>()) {
}

unique_ptr<SQLStatement> ShowStatement::Copy() const {
	auto result = make_unique<ShowStatement>();
	result->info = info->Copy();
	return result;
}

} // namespace duckdb
