#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

ExplainStatement::ExplainStatement(unique_ptr<SQLStatement> stmt, ExplainType explain_type,
                                   const ProfilerPrintFormat &explain_format)
    : SQLStatement(StatementType::EXPLAIN_STATEMENT), stmt(std::move(stmt)), explain_type(explain_type),
      explain_format(explain_format) {
}

ExplainStatement::ExplainStatement(const ExplainStatement &other)
    : SQLStatement(other), stmt(other.stmt->Copy()), explain_type(other.explain_type),
      explain_format(other.explain_format) {
}

unique_ptr<SQLStatement> ExplainStatement::Copy() const {
	return unique_ptr<ExplainStatement>(new ExplainStatement(*this));
}

string ExplainStatement::OptionsToString() const {
	string options;
	if (explain_type == ExplainType::EXPLAIN_ANALYZE) {
		options += "(";
		options += "ANALYZE";
	}
	if (explain_format != ProfilerPrintFormat::DEFAULT()) {
		if (options.empty()) {
			options += "(";
		} else {
			options += ", ";
		}
		options += StringUtil::Format("FORMAT %s", StringUtil::Upper(explain_format.ToString()));
	}
	if (!options.empty()) {
		options += ")";
	}
	return options;
}

string ExplainStatement::ToString() const {
	string result = "EXPLAIN";
	auto options = OptionsToString();
	if (!options.empty()) {
		result += " " + options;
	}
	result += " " + stmt->ToString();
	return result;
}

} // namespace duckdb
