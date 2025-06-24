#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

SetStatement::SetStatement(string name_p, SetScope scope_p, SetType type_p)
    : SQLStatement(StatementType::SET_STATEMENT), name(std::move(name_p)), scope(scope_p), set_type(type_p) {
}

// Set Variable

SetVariableStatement::SetVariableStatement(string name_p, unique_ptr<ParsedExpression> value_p, SetScope scope_p)
    : SetStatement(std::move(name_p), scope_p, SetType::SET), value(std::move(value_p)) {
}

SetVariableStatement::SetVariableStatement(const SetVariableStatement &other)
    : SetVariableStatement(other.name, other.value->Copy(), other.scope) {
}

unique_ptr<SQLStatement> SetVariableStatement::Copy() const {
	return unique_ptr<SetVariableStatement>(new SetVariableStatement(*this));
}

static string ScopeToString(SetScope scope) {
	switch (scope) {
	case SetScope::AUTOMATIC:
		return "";
	case SetScope::LOCAL:
		return "LOCAL";
	case SetScope::SESSION:
		return "SESSION";
	case SetScope::GLOBAL:
		return "GLOBAL";
	case SetScope::VARIABLE:
		return "VARIABLE";
	default:
		throw InternalException("ToString not implemented for SetScope of type: %s", EnumUtil::ToString(scope));
	}
}

string SetVariableStatement::ToString() const {
	return StringUtil::Format("SET %s %s TO %s;", ScopeToString(scope), name, value->ToString());
}

// Reset Variable

ResetVariableStatement::ResetVariableStatement(std::string name_p, SetScope scope_p)
    : SetStatement(std::move(name_p), scope_p, SetType::RESET) {
}

unique_ptr<SQLStatement> ResetVariableStatement::Copy() const {
	return unique_ptr<ResetVariableStatement>(new ResetVariableStatement(*this));
}

string ResetVariableStatement::ToString() const {
	string result = "";
	result += "RESET";
	result += " " + ScopeToString(scope);
	result += " " + name;
	result += ";";
	return result;
}

} // namespace duckdb
