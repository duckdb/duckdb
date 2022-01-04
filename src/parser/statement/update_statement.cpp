#include "duckdb/parser/statement/update_statement.hpp"

namespace duckdb {

UpdateStatement::UpdateStatement() : SQLStatement(StatementType::UPDATE_STATEMENT) {
}

unique_ptr<SQLStatement> UpdateStatement::Copy() const {
	auto result = make_unique<UpdateStatement>();
	if (condition) {
		result->condition = condition->Copy();
	}
	result->table = table->Copy();
	if (from_table) {
		result->from_table = from_table->Copy();
	}
	result->columns = columns;
	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
	return result;
}

} // namespace duckdb
