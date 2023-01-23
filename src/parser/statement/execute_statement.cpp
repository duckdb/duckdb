#include "duckdb/parser/statement/execute_statement.hpp"

namespace duckdb {

ExecuteStatement::ExecuteStatement() : SQLStatement(StatementType::EXECUTE_STATEMENT) {
}

ExecuteStatement::ExecuteStatement(const ExecuteStatement &other) : SQLStatement(other), name(other.name) {
	for (const auto &value : other.values) {
		values.push_back(value->Copy());
	}
}

unique_ptr<SQLStatement> ExecuteStatement::Copy() const {
	return unique_ptr<ExecuteStatement>(new ExecuteStatement(*this));
}

bool ExecuteStatement::Equals(const SQLStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (const ExecuteStatement &)*other_p;
	if (other.name != name) {
		return false;
	}
	if (values.size() != other.values.size()) {
		return false;
	}
	for (idx_t i = 0; i < values.size(); i++) {
		auto &lhs = values[i];
		auto &rhs = other.values[i];
		if (!lhs->Equals(rhs.get())) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
