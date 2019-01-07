#include "parser/query_node/select_node.hpp"

#include "common/enums.hpp"

using namespace duckdb;
using namespace std;

bool SelectNode::HasAggregation() {
	if (HasGroup()) {
		return true;
	}
	for (auto &expr : select_list) {
		if (expr->IsAggregate()) {
			return true;
		}
	}
	return false;
}

bool SelectNode::HasWindow() {
	for (auto &expr : select_list) {
		if (expr->IsWindow()) {
			return true;
		}
	}
	return false;
}

bool SelectNode::Equals(const QueryNode *other_) const {
	if (!QueryNode::Equals(other_)) {
		return false;
	}
	auto other = (SelectNode *)other_;

	// first check counts of all lists and such
	if (select_list.size() != other->select_list.size() || select_distinct != other->select_distinct ||
	    orderby.orders.size() != other->orderby.orders.size() ||
	    groupby.groups.size() != other->groupby.groups.size() || limit.limit != other->limit.limit ||
	    limit.offset != other->limit.offset) {
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
	if (!Expression::Equals(where_clause.get(), other->where_clause.get())) {
		return false;
	}
	// GROUP BY
	for (size_t i = 0; i < groupby.groups.size(); i++) {
		if (!groupby.groups[i]->Equals(other->groupby.groups[i].get())) {
			return false;
		}
	}
	// HAVING
	if (!Expression::Equals(groupby.having.get(), other->groupby.having.get())) {
		return false;
	}

	// ORDERS
	for (size_t i = 0; i < orderby.orders.size(); i++) {
		if (orderby.orders[i].type != orderby.orders[i].type ||
		    !orderby.orders[i].expression->Equals(other->orderby.orders[i].expression.get())) {
			return false;
		}
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
	return result;
}

void SelectNode::Serialize(Serializer &serializer) {
	QueryNode::Serialize(serializer);
	// select_list
	serializer.WriteList(select_list);
	// from clause
	serializer.WriteOptional(from_table);
	// where_clause
	serializer.WriteOptional(where_clause);
	// select_distinct
	serializer.Write<bool>(select_distinct);
	// group by
	serializer.WriteList(groupby.groups);
	// having
	serializer.WriteOptional(groupby.having);
	// order by
	serializer.Write<uint32_t>(orderby.orders.size());
	for (auto &order : orderby.orders) {
		serializer.Write<OrderType>(order.type);
		order.expression->Serialize(serializer);
	}
	// limit
	serializer.Write<int64_t>(limit.limit);
	serializer.Write<int64_t>(limit.offset);
}

unique_ptr<QueryNode> SelectNode::Deserialize(Deserializer &source) {
	auto result = make_unique<SelectNode>();
	// select_list
	source.ReadList<Expression>(result->select_list);
	// from clause
	result->from_table = source.ReadOptional<TableRef>();
	// where_clause
	result->where_clause = source.ReadOptional<Expression>();
	// select_distinct
	result->select_distinct = source.Read<bool>();
	// group by
	source.ReadList<Expression>(result->groupby.groups);
	// having
	result->groupby.having = source.ReadOptional<Expression>();
	// order by
	auto order_count = source.Read<uint32_t>();
	for (size_t i = 0; i < order_count; i++) {
		auto order_type = source.Read<OrderType>();
		auto expression = Expression::Deserialize(source);
		result->orderby.orders.push_back(OrderByNode(order_type, move(expression)));
	}
	// limit
	result->limit.limit = source.Read<int64_t>();
	result->limit.offset = source.Read<int64_t>();
	return result;
}
