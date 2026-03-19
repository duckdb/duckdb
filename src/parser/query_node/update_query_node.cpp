#include "duckdb/parser/query_node/update_query_node.hpp"

namespace duckdb {

UpdateQueryNode::UpdateQueryNode() : QueryNode(QueryNodeType::UPDATE_QUERY_NODE) {
}

UpdateQueryNode::UpdateQueryNode(UpdateStatement &stmt) : QueryNode(QueryNodeType::UPDATE_QUERY_NODE) {
	if (stmt.table) {
		table = stmt.table->Copy();
	}
	if (stmt.from_table) {
		from_table = stmt.from_table->Copy();
	}
	for (auto &expr : stmt.returning_list) {
		returning_list.push_back(expr->Copy());
	}
	if (stmt.set_info) {
		set_info = stmt.set_info->Copy();
	}
}

string UpdateQueryNode::ToString() const {
	UpdateStatement stmt;
	if (table) {
		stmt.table = table->Copy();
	}
	if (from_table) {
		stmt.from_table = from_table->Copy();
	}
	for (auto &expr : returning_list) {
		stmt.returning_list.push_back(expr->Copy());
	}
	if (set_info) {
		stmt.set_info = set_info->Copy();
	}
	return stmt.ToString();
}

bool UpdateQueryNode::Equals(const QueryNode *other_p) const {
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	if (this == other_p) {
		return true;
	}
	auto &other = other_p->Cast<UpdateQueryNode>();
	if (!TableRef::Equals(table, other.table)) {
		return false;
	}
	if (!TableRef::Equals(from_table, other.from_table)) {
		return false;
	}
	// compare set_info fields directly (no Equals on UpdateSetInfo)
	if ((set_info == nullptr) != (other.set_info == nullptr)) {
		return false;
	}
	if (set_info) {
		if (set_info->columns != other.set_info->columns) {
			return false;
		}
		if (set_info->expressions.size() != other.set_info->expressions.size()) {
			return false;
		}
		for (idx_t i = 0; i < set_info->expressions.size(); i++) {
			if (!ParsedExpression::Equals(set_info->expressions[i], other.set_info->expressions[i])) {
				return false;
			}
		}
		if (!ParsedExpression::Equals(set_info->condition, other.set_info->condition)) {
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

unique_ptr<QueryNode> UpdateQueryNode::Copy() const {
	auto result = make_uniq<UpdateQueryNode>();
	if (table) {
		result->table = table->Copy();
	}
	if (from_table) {
		result->from_table = from_table->Copy();
	}
	for (auto &expr : returning_list) {
		result->returning_list.push_back(expr->Copy());
	}
	if (set_info) {
		result->set_info = set_info->Copy();
	}
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
