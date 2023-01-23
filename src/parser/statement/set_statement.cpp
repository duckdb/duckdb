#include "duckdb/parser/statement/set_statement.hpp"

namespace duckdb {

SetStatement::SetStatement(std::string name_p, SetScope scope_p, SetType type_p)
    : SQLStatement(StatementType::SET_STATEMENT), name(std::move(name_p)), scope(scope_p), set_type(type_p) {
}

unique_ptr<SQLStatement> SetStatement::Copy() const {
	return unique_ptr<SetStatement>(new SetStatement(*this));
}

bool SetStatement::Equals(const SQLStatement *other_p) const {
	if (other_p->type != type) {
		return false;
	}
	auto &other = (const SetStatement &)*other_p;
	if (name != other.name) {
		return false;
	}
	if (scope != other.scope) {
		return false;
	}
	if (set_type != other.set_type) {
		return false;
	}
	return true;
}

// Set Variable

SetVariableStatement::SetVariableStatement(std::string name_p, Value value_p, SetScope scope_p)
    : SetStatement(std::move(name_p), scope_p, SetType::SET), value(std::move(value_p)) {
}

unique_ptr<SQLStatement> SetVariableStatement::Copy() const {
	return unique_ptr<SetVariableStatement>(new SetVariableStatement(*this));
}

// Reset Variable

ResetVariableStatement::ResetVariableStatement(std::string name_p, SetScope scope_p)
    : SetStatement(std::move(name_p), scope_p, SetType::RESET) {
}

} // namespace duckdb
