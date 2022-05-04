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

string DeleteStatement::ToString() const {
	string result;
	result += "DELETE FROM ";
	result += table->ToString();
	if (!using_clauses.empty()) {
		result += " USING ";
		for (idx_t i = 0; i < using_clauses.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += using_clauses[i]->ToString();
		}
	}
	if (condition) {
		result += " WHERE " + condition->ToString();
	}

	if (!returning_list.empty()) {
		result += " RETURNING ";
		for (idx_t i = 0; i < returning_list.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += returning_list[i]->ToString();
		}
	}
	return result;
}

unique_ptr<SQLStatement> DeleteStatement::Copy() const {
	return unique_ptr<DeleteStatement>(new DeleteStatement(*this));
}

} // namespace duckdb
