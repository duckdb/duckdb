#include "parser/query_node/select_node.hpp"

using namespace duckdb;
using namespace std;

bool SelectNode::Equals(const QueryNode *other_) const {
	if (!QueryNode::Equals(other_)) {
		return false;
	}
	if (this == other_) {
		return true;
	}
	auto other = (SelectNode *)other_;

	// first check counts of all lists and such
	if (select_list.size() != other->select_list.size() || select_distinct != other->select_distinct ||
	    orderby.orders.size() != other->orderby.orders.size() ||
	    groupby.groups.size() != other->groupby.groups.size()) {
		return false;
	}
	// SELECT
	for (size_t i = 0; i < select_list.size(); i++) {
		if (!select_list[i]->Equals(other->select_list[i].get())) {
			return false;
		}
	}
	// FROM
	if (from_table) {
		// we have a FROM clause, compare to the other one
		if (!from_table->Equals(other->from_table.get())) {
			return false;
		}
	} else if (other->from_table) {
		// we don't have a FROM clause, if the other statement has one they are
		// not equal
		return false;
	}
	// WHERE
	if (!ParsedExpression::Equals(where_clause.get(), other->where_clause.get())) {
		return false;
	}
	// GROUP BY
	for (size_t i = 0; i < groupby.groups.size(); i++) {
		if (!groupby.groups[i]->Equals(other->groupby.groups[i].get())) {
			return false;
		}
	}
	// HAVING
	if (!ParsedExpression::Equals(groupby.having.get(), other->groupby.having.get())) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> SelectNode::Copy() {
	auto result = make_unique<SelectNode>();
	for (auto &child : select_list) {
		result->select_list.push_back(child->Copy());
	}
	result->from_table = from_table ? from_table->Copy() : nullptr;
	result->where_clause = where_clause ? where_clause->Copy() : nullptr;
	// groups
	for (auto &group : groupby.groups) {
		result->groupby.groups.push_back(group->Copy());
	}
	result->groupby.having = groupby.having ? groupby.having->Copy() : nullptr;
	this->CopyProperties(*result);
	return move(result);
}

void SelectNode::Serialize(Serializer &serializer) {
	QueryNode::Serialize(serializer);
	// select_list
	serializer.WriteList(select_list);
	// from clause
	serializer.WriteOptional(from_table);
	// where_clause
	serializer.WriteOptional(where_clause);
	// group by / having
	serializer.WriteList(groupby.groups);
	serializer.WriteOptional(groupby.having);
}

unique_ptr<QueryNode> SelectNode::Deserialize(Deserializer &source) {
	auto result = make_unique<SelectNode>();
	// select_list
	source.ReadList<ParsedExpression>(result->select_list);
	// from clause
	result->from_table = source.ReadOptional<TableRef>();
	// where_clause
	result->where_clause = source.ReadOptional<ParsedExpression>();
	// group by / having
	source.ReadList<ParsedExpression>(result->groupby.groups);
	result->groupby.having = source.ReadOptional<ParsedExpression>();
	return move(result);
}
