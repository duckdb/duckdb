#include "duckdb/parser/statement/update_statement.hpp"

namespace duckdb {

UpdateSetInfo::UpdateSetInfo() {
}

UpdateSetInfo::UpdateSetInfo(const UpdateSetInfo &other) : columns(other.columns) {
	if (other.condition) {
		condition = other.condition->Copy();
	}
	for (auto &expr : other.expressions) {
		expressions.emplace_back(expr->Copy());
	}
}

string UpdateSetInfo::ToString() const {
	string result;
	result += "SET ";
	D_ASSERT(columns.size() == expressions.size());
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += KeywordHelper::WriteOptionallyQuoted(columns[i]);
		result += " = ";
		result += expressions[i]->ToString();
	}
	return result;
}

unique_ptr<UpdateSetInfo> UpdateSetInfo::Copy() const {
	return unique_ptr<UpdateSetInfo>(new UpdateSetInfo(*this));
}

UpdateStatement::UpdateStatement() : SQLStatement(StatementType::UPDATE_STATEMENT) {
}

UpdateStatement::UpdateStatement(const UpdateStatement &other)
    : SQLStatement(other), table(other.table->Copy()), set_info(other.set_info->Copy()) {
	if (other.from_table) {
		from_table = other.from_table->Copy();
	}
	for (auto &expr : other.returning_list) {
		returning_list.emplace_back(expr->Copy());
	}
	cte_map = other.cte_map.Copy();
}

string UpdateStatement::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "UPDATE ";
	result += table->ToString();
	result += " ";
	result += set_info->ToString();
	if (from_table) {
		result += " FROM " + from_table->ToString();
	}
	if (set_info->condition) {
		result += " WHERE " + set_info->condition->ToString();
	}
	if (!returning_list.empty()) {
		result += " RETURNING ";
		for (idx_t i = 0; i < returning_list.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			auto column = returning_list[i]->ToString();
			if (!returning_list[i]->GetAlias().empty()) {
				column +=
				    StringUtil::Format(" AS %s", KeywordHelper::WriteOptionallyQuoted(returning_list[i]->GetAlias()));
			}
			result += column;
		}
	}
	return result;
}

unique_ptr<SQLStatement> UpdateStatement::Copy() const {
	return unique_ptr<UpdateStatement>(new UpdateStatement(*this));
}

} // namespace duckdb
