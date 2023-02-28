#include "duckdb/parser/statement/multi_statement.hpp"

namespace duckdb {

MultiStatement::MultiStatement() : SQLStatement(StatementType::MULTI_STATEMENT) {
}

MultiStatement::MultiStatement(const MultiStatement &other) : SQLStatement(other) {
	for (auto &stmt : other.statements) {
		statements.push_back(stmt->Copy());
	}
}

unique_ptr<SQLStatement> MultiStatement::Copy() const {
	return unique_ptr<MultiStatement>(new MultiStatement(*this));
}

bool MultiStatement::Equals(const SQLStatement *other_p) const {
	if (other_p->type != type) {
		return false;
	}
	auto &other = (const MultiStatement &)*other_p;
	if (other.statements.size() != statements.size()) {
		return false;
	}
	for (idx_t i = 0; i < statements.size(); i++) {
		auto &this_statement = statements[i];
		auto &other_statement = other.statements[i];
		D_ASSERT(this_statement);
		if (!this_statement->Equals(other_statement.get())) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
