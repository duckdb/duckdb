#include "duckdb/parser/statement/call_statement.hpp"

namespace duckdb {

CallStatement::CallStatement() : SQLStatement(StatementType::CALL_STATEMENT) {
}

CallStatement::CallStatement(const CallStatement &other) : SQLStatement(other), function(other.function->Copy()) {
}

unique_ptr<SQLStatement> CallStatement::Copy() const {
	return unique_ptr<CallStatement>(new CallStatement(*this));
}

bool CallStatement::Equals(const SQLStatement *other_p) const {
	if (other_p->type != type) {
		return false;
	}
	auto &other = (const CallStatement &)*other_p;
	if (!function && !other.function) {
		return true;
	}
	if (!function || !other.function) {
		return false;
	}
	return function->Equals(other.function.get());
}

} // namespace duckdb
