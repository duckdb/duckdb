#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

DeleteQueryNode::DeleteQueryNode() : QueryNode(QueryNodeType::DELETE_QUERY_NODE) {
}

string DeleteQueryNode::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "DELETE FROM ";
	result += table->ToString();
	if (!using_clauses.empty()) {
		result += " USING ";
		for (idx_t i = 0; i < using_clauses.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += using_clauses[i]->ToString();
		}
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
			auto col = returning_list[i]->ToString();
			if (!returning_list[i]->GetAlias().empty()) {
				col +=
				    StringUtil::Format(" AS %s", SQLIdentifier(returning_list[i]->GetAlias()));
			}
			result += col;
		}
	}
	return result;
}

bool DeleteQueryNode::Equals(const QueryNode *other_p) const {
	if (this == other_p) {
		return true;
	}
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	auto &other = other_p->Cast<DeleteQueryNode>();
	if (!TableRef::Equals(table, other.table)) {
		return false;
	}
	if (!ParsedExpression::Equals(condition, other.condition)) {
		return false;
	}
	if (using_clauses.size() != other.using_clauses.size()) {
		return false;
	}
	for (idx_t i = 0; i < using_clauses.size(); i++) {
		if (!TableRef::Equals(using_clauses[i], other.using_clauses[i])) {
			return false;
		}
	}
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
	result->table = table->Copy();
	if (condition) {
		result->condition = condition->Copy();
	}
	for (auto &using_clause : using_clauses) {
		result->using_clauses.push_back(using_clause->Copy());
	}
	for (auto &expr : returning_list) {
		result->returning_list.push_back(expr->Copy());
	}
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
