#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression_util.hpp"

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

	// SELECT
	if (!ExpressionUtil::ListEquals(select_list, other->select_list)) {
		return false;
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
	if (!BaseExpression::Equals(where_clause.get(), other->where_clause.get())) {
		return false;
	}
	// GROUP BY
	if (!ExpressionUtil::ListEquals(groups, other->groups)) {
		return false;
	}

	// HAVING
	if (!BaseExpression::Equals(having.get(), other->having.get())) {
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
	for (auto &group : groups) {
		result->groups.push_back(group->Copy());
	}
	result->having = having ? having->Copy() : nullptr;
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
	serializer.WriteList(groups);
	serializer.WriteOptional(having);
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
	source.ReadList<ParsedExpression>(result->groups);
	result->having = source.ReadOptional<ParsedExpression>();
	return move(result);
}
