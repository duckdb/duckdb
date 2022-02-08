#include "duckdb/parser/statement/delete_statement.hpp"

namespace duckdb {

DeleteStatement::DeleteStatement() : SQLStatement(StatementType::DELETE_STATEMENT) {
}

DeleteStatement::DeleteStatement(const DeleteStatement &other) : SQLStatement(other), table(other.table->Copy()) {
	if (other.condition) {
		condition = other.condition->Copy();
	}
	for (const auto &using_clause : other.using_clauses) {
		using_clauses.push_back(using_clause->Copy());
	}
}

unique_ptr<SQLStatement> DeleteStatement::Copy() const {
	return unique_ptr<DeleteStatement>(new DeleteStatement(*this));
}

} // namespace duckdb
