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

string MultiStatement::ToString() const {
	vector<string> stringified;
	for (auto &stmt : statements) {
		stringified.push_back(stmt->ToString());
	}
	return StringUtil::Join(stringified, ";") + ";";
}

} // namespace duckdb
