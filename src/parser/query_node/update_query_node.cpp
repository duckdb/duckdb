#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

UpdateQueryNode::UpdateQueryNode() : QueryNode(QueryNodeType::UPDATE_QUERY_NODE) {
}

string UpdateQueryNode::ToString() const {
	D_ASSERT(table && set_info);
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
			auto col = returning_list[i]->ToString();
			if (!returning_list[i]->GetAlias().empty()) {
				col +=
				    StringUtil::Format(" AS %s", KeywordHelper::WriteOptionallyQuoted(returning_list[i]->GetAlias()));
			}
			result += col;
		}
	}
	return result;
}

bool UpdateQueryNode::Equals(const QueryNode *other_p) const {
	if (this == other_p) {
		return true;
	}
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	auto &other = other_p->Cast<UpdateQueryNode>();
	if (!TableRef::Equals(table, other.table)) {
		return false;
	}
	if (!TableRef::Equals(from_table, other.from_table)) {
		return false;
	}
	if (!UpdateSetInfo::Equals(set_info, other.set_info)) {
		return false;
	}
	if (returning_list.size() != other.returning_list.size()) {
		return false;
	}
	for (idx_t i = 0; i < returning_list.size(); i++) {
		if (!ParsedExpression::Equals(returning_list[i], other.returning_list[i])) {
			return false;
		}
	}
	if (prioritize_table_when_binding != other.prioritize_table_when_binding) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> UpdateQueryNode::Copy() const {
	auto result = make_uniq<UpdateQueryNode>();
	result->table = table->Copy();
	if (from_table) {
		result->from_table = from_table->Copy();
	}
	for (auto &expr : returning_list) {
		result->returning_list.push_back(expr->Copy());
	}
	result->set_info = set_info->Copy();
	result->prioritize_table_when_binding = prioritize_table_when_binding;
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
