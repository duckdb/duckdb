#include "duckdb/parser/statement/execute_statement.hpp"

namespace duckdb {

ExecuteStatement::ExecuteStatement() : SQLStatement(StatementType::EXECUTE_STATEMENT) {
}

ExecuteStatement::ExecuteStatement(const ExecuteStatement &other)
    : SQLStatement(other), name(other.name), bound_values(other.bound_values) {
	for (const auto &item : other.named_values) {
		named_values.emplace(std::make_pair(item.first, item.second->Copy()));
	}
}

unique_ptr<SQLStatement> ExecuteStatement::Copy() const {
	return unique_ptr<ExecuteStatement>(new ExecuteStatement(*this));
}

string ExecuteStatement::ToString() const {
	string result = "";
	result += "EXECUTE";
	result += " " + name;
	vector<string> stringified;
	for (auto &val : named_values) {
		stringified.push_back(StringUtil::Format("%s := %s", val.first, val.second->ToString()));
	}
	for (auto &val : bound_values) {
		stringified.push_back(StringUtil::Format("%s := %s", val.first, val.second.GetValue().ToSQLString()));
	}
	if (!stringified.empty()) {
		result += "(" + StringUtil::Join(stringified, ", ") + ")";
	}
	result += ";";
	return result;
}

} // namespace duckdb
