#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

ExplainStatement::ExplainStatement(unique_ptr<SQLStatement> stmt, ExplainType explain_type)
    : SQLStatement(StatementType::EXPLAIN_STATEMENT), stmt(std::move(stmt)), explain_type(explain_type) {
}

ExplainStatement::ExplainStatement(const ExplainStatement &other)
    : SQLStatement(other), stmt(other.stmt->Copy()), explain_type(other.explain_type) {
}

unique_ptr<SQLStatement> ExplainStatement::Copy() const {
	return unique_ptr<ExplainStatement>(new ExplainStatement(*this));
}

static string ExplainTypeToString(ExplainType type) {
	switch (type) {
	case ExplainType::EXPLAIN_STANDARD:
		return "EXPLAIN";
	case ExplainType::EXPLAIN_ANALYZE:
		return "EXPLAIN ANALYZE";
	default:
		throw InternalException("ToString for ExplainType with type: %s not implemented", EnumUtil::ToString(type));
	}
}

string ExplainStatement::ToString() const {
	string result = "";
	result += ExplainTypeToString(explain_type);
	result += " " + stmt->ToString();
	return result;
}

} // namespace duckdb
