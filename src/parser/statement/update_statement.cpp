#include "duckdb/parser/statement/update_statement.hpp"

namespace duckdb {

UpdateStatement::UpdateStatement() : SQLStatement(StatementType::UPDATE_STATEMENT) {
}

UpdateStatement::UpdateStatement(const UpdateStatement &other)
    : SQLStatement(other), table(other.table->Copy()), columns(other.columns) {
	if (other.condition) {
		condition = other.condition->Copy();
	}
	if (other.from_table) {
		from_table = other.from_table->Copy();
	}
	for (auto &expr : other.expressions) {
		expressions.emplace_back(expr->Copy());
	}
}

unique_ptr<SQLStatement> UpdateStatement::Copy() const {
	return unique_ptr<UpdateStatement>(new UpdateStatement(*this));
}

} // namespace duckdb
