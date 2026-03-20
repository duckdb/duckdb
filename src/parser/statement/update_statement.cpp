#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/keyword_helper.hpp"

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

bool UpdateSetInfo::Equals(const unique_ptr<UpdateSetInfo> &a, const unique_ptr<UpdateSetInfo> &b) {
	if (!a && !b) {
		return true;
	}
	if (!a || !b) {
		return false;
	}
	if (a->columns != b->columns) {
		return false;
	}
	if (a->expressions.size() != b->expressions.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->expressions.size(); i++) {
		if (!ParsedExpression::Equals(a->expressions[i], b->expressions[i])) {
			return false;
		}
	}
	return ParsedExpression::Equals(a->condition, b->condition);
}

UpdateStatement::UpdateStatement() : SQLStatement(StatementType::UPDATE_STATEMENT), node(make_uniq<UpdateQueryNode>()) {
}

UpdateStatement::UpdateStatement(const UpdateStatement &other)
    : SQLStatement(other), node(unique_ptr_cast<QueryNode, UpdateQueryNode>(other.node->Copy())) {
}

string UpdateStatement::ToString() const {
	return node->ToString();
}

unique_ptr<SQLStatement> UpdateStatement::Copy() const {
	return unique_ptr<UpdateStatement>(new UpdateStatement(*this));
}

} // namespace duckdb
