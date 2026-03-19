#include "duckdb/parser/query_node/delete_query_node.hpp"

namespace duckdb {

DeleteQueryNode::DeleteQueryNode() : QueryNode(QueryNodeType::DELETE_QUERY_NODE) {
}

DeleteQueryNode::DeleteQueryNode(DeleteStatement &stmt) : QueryNode(QueryNodeType::DELETE_QUERY_NODE) {
	if (stmt.condition) {
		condition = stmt.condition->Copy();
	}
	if (stmt.table) {
		table = stmt.table->Copy();
	}
	for (auto &clause : stmt.using_clauses) {
		using_clauses.push_back(clause->Copy());
	}
	for (auto &expr : stmt.returning_list) {
		returning_list.push_back(expr->Copy());
	}
}

string DeleteQueryNode::ToString() const {
	DeleteStatement stmt;
	if (condition) {
		stmt.condition = condition->Copy();
	}
	if (table) {
		stmt.table = table->Copy();
	}
	for (auto &clause : using_clauses) {
		stmt.using_clauses.push_back(clause->Copy());
	}
	for (auto &expr : returning_list) {
		stmt.returning_list.push_back(expr->Copy());
	}
	return stmt.ToString();
}

bool DeleteQueryNode::Equals(const QueryNode *other_p) const {
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	if (this == other_p) {
		return true;
	}
	auto &other = other_p->Cast<DeleteQueryNode>();
	if (!TableRef::Equals(table, other.table)) {
		return false;
	}
	if (!ParsedExpression::Equals(condition, other.condition)) {
		return false;
	}
	// compare using_clauses
	if (using_clauses.size() != other.using_clauses.size()) {
		return false;
	}
	for (idx_t i = 0; i < using_clauses.size(); i++) {
		if (!TableRef::Equals(using_clauses[i], other.using_clauses[i])) {
			return false;
		}
	}
	// compare returning_list
	if (returning_list.size() != other.returning_list.size()) {
		return false;
	}
	for (idx_t i = 0; i < returning_list.size(); i++) {
		if (!ParsedExpression::Equals(returning_list[i], other.returning_list[i])) {
			return false;
		}
	}
	return true;
}

unique_ptr<QueryNode> DeleteQueryNode::Copy() const {
	auto result = make_uniq<DeleteQueryNode>();
	if (condition) {
		result->condition = condition->Copy();
	}
	if (table) {
		result->table = table->Copy();
	}
	for (auto &clause : using_clauses) {
		result->using_clauses.push_back(clause->Copy());
	}
	for (auto &expr : returning_list) {
		result->returning_list.push_back(expr->Copy());
	}
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
