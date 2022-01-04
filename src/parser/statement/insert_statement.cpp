#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

InsertStatement::InsertStatement() : SQLStatement(StatementType::INSERT_STATEMENT), schema(DEFAULT_SCHEMA) {
}

unique_ptr<SQLStatement> InsertStatement::Copy() const {
	auto result = make_unique<InsertStatement>();
	result->select_statement = unique_ptr_cast<SQLStatement, SelectStatement>(select_statement->Copy());
	result->columns = columns;
	result->table = table;
	result->schema = schema;
	return result;
}

} // namespace duckdb
