#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

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

bool UpdateSetInfo::Equals(const UpdateSetInfo &other) const {
	if (!condition && !other.condition) {
	} else if (!condition || !other.condition) {
		return false;
	} else if (!condition->Equals(other.condition.get())) {
		return false;
	}

	if (columns != other.columns) {
		return false;
	}

	if (expressions.size() != other.expressions.size()) {
		return false;
	}
	for (idx_t i = 0; i < expressions.size(); i++) {
		auto &lhs = expressions[i];
		auto &rhs = other.expressions[i];

		if (!lhs->Equals(rhs.get())) {
			return false;
		}
	}
	return true;
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
	cte_map = other.cte_map.Copy();
}

string UpdateStatement::ToString() const {
	D_ASSERT(set_info);
	auto &condition = set_info->condition;
	auto &columns = set_info->columns;
	auto &expressions = set_info->expressions;

	string result;
	result = cte_map.ToString();
	result += "UPDATE ";
	result += table->ToString();
	result += " SET ";
	D_ASSERT(columns.size() == expressions.size());
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += KeywordHelper::WriteOptionallyQuoted(columns[i]);
		result += " = ";
		result += expressions[i]->ToString();
	}
	if (from_table) {
		result += " FROM " + from_table->ToString();
	}
	if (condition) {
		result += " WHERE " + condition->ToString();
	}
	if (!returning_list.empty()) {
		result += " RETURNING ";
		for (idx_t i = 0; i < returning_list.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += returning_list[i]->ToString();
		}
	}
	return result;
}

unique_ptr<SQLStatement> UpdateStatement::Copy() const {
	return unique_ptr<UpdateStatement>(new UpdateStatement(*this));
}

bool UpdateStatement::Equals(const SQLStatement *other_p) const {
	if (other_p->type != type) {
		return false;
	}
	auto &other = (const UpdateStatement &)*other_p;

	D_ASSERT(other.set_info);
	if (!set_info->Equals(*other.set_info)) {
		return false;
	}

	D_ASSERT(table);
	if (!table->Equals(other.table.get())) {
		return false;
	}

	if (!from_table && !other.from_table) {
	} else if (!from_table || !other.from_table) {
		return false;
	} else if (!from_table->Equals(other.from_table.get())) {
		return false;
	}

	if (returning_list.size() != other.returning_list.size()) {
		return false;
	}
	for (idx_t i = 0; i < returning_list.size(); i++) {
		auto &lhs = returning_list[i];
		auto &rhs = other.returning_list[i];

		if (!lhs->Equals(rhs.get())) {
			return false;
		}
	}

	return cte_map.Equals(other.cte_map);
}

} // namespace duckdb
