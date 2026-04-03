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

bool UpdateSetInfo::Equals(const unique_ptr<UpdateSetInfo> &left, const unique_ptr<UpdateSetInfo> &right) {
	if (!left && !right) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	if (left->columns != right->columns) {
		return false;
	}
	if (left->expressions.size() != right->expressions.size()) {
		return false;
	}
	for (idx_t i = 0; i < left->expressions.size(); i++) {
		if (!ParsedExpression::Equals(left->expressions[i], right->expressions[i])) {
			return false;
		}
	}
	return ParsedExpression::Equals(left->condition, right->condition);
}

UpdateStatement::UpdateStatement() : SQLStatement(StatementType::UPDATE_STATEMENT), node(make_uniq<UpdateQueryNode>()) {
}

UpdateStatement::~UpdateStatement() {
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
