#include "duckdb/parser/statement/external_resource_statement.hpp"

#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

ExternalResourceStatement::ExternalResourceStatement(ExternalResourceOperation operation)
    : SQLStatement(StatementType::EXTERNAL_RESOURCE_STATEMENT), operation(operation) {
}

ExternalResourceStatement::ExternalResourceStatement(const ExternalResourceStatement &other)
    : SQLStatement(other), operation(other.operation), type(other.type), name(other.name), all(other.all) {
	if (other.handle) {
		handle = other.handle->Copy();
	}
	for (auto &entry : other.options) {
		options[entry.first] = entry.second->Copy();
	}
}

unique_ptr<SQLStatement> ExternalResourceStatement::Copy() const {
	return unique_ptr<ExternalResourceStatement>(new ExternalResourceStatement(*this));
}

string ExternalResourceStatement::ToString() const {
	string result;
	switch (operation) {
	case ExternalResourceOperation::CREATE:
		result = "CREATE EXTERNAL RESOURCE " + SQLString(type);
		if (!name.GetIdentifierName().empty()) {
			result += " AS " + name.GetIdentifierName();
		}
		break;
	case ExternalResourceOperation::REGISTER:
		result = "REGISTER EXTERNAL RESOURCE " + SQLString(type);
		if (!name.GetIdentifierName().empty()) {
			result += " AS " + name.GetIdentifierName();
		}
		result += " FROM " + (handle ? handle->ToString() : string());
		break;
	case ExternalResourceOperation::DESTROY:
		result = "DESTROY EXTERNAL RESOURCE " + name.GetIdentifierName();
		break;
	case ExternalResourceOperation::SHOW:
		result = all ? "SHOW ALL EXTERNAL RESOURCES" : "SHOW EXTERNAL RESOURCES";
		break;
	}
	return result;
}

} // namespace duckdb
