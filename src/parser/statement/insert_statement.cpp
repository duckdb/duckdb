#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

InsertStatement::InsertStatement() : SQLStatement(StatementType::INSERT_STATEMENT), schema(DEFAULT_SCHEMA) {
}

InsertStatement::InsertStatement(const InsertStatement &other)
    : SQLStatement(other),
      select_statement(unique_ptr_cast<SQLStatement, SelectStatement>(other.select_statement->Copy())),
      columns(other.columns), table(other.table), schema(other.schema) {
}

unique_ptr<SQLStatement> InsertStatement::Copy() const {
	return unique_ptr<InsertStatement>(new InsertStatement(*this));
}

} // namespace duckdb
