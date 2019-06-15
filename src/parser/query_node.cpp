#include "parser/query_node.hpp"

#include "parser/query_node/select_node.hpp"
#include "parser/query_node/set_operation_node.hpp"

using namespace duckdb;
using namespace std;

bool QueryNode::Equals(const QueryNode *other) const {
	if (!other) {
		return false;
	}
	if (this == other) {
		return true;
	}
	if (other->type != this->type) {
		return false;
	}
	if (select_distinct != other->select_distinct) {
		return false;
	}
	if (!ParsedExpression::Equals(limit.get(), other->limit.get())) {
		return false;
	}
	if (!ParsedExpression::Equals(offset.get(), other->offset.get())) {
		return false;
	}
	if (orders.size() != other->orders.size()) {
		return false;
	}
	for (index_t i = 0; i < orders.size(); i++) {
		if (orders[i].type != other->orders[i].type ||
		    !orders[i].expression->Equals(other->orders[i].expression.get())) {
			return false;
		}
	}
	return other->type == type;
}

void QueryNode::CopyProperties(QueryNode &other) {
	other.select_distinct = select_distinct;
	// order
	for (auto &order : orders) {
		other.orders.push_back(OrderByNode(order.type, order.expression->Copy()));
	}
	// limit
	other.limit = limit ? limit->Copy() : nullptr;
	other.offset = offset ? offset->Copy() : nullptr;
}

void QueryNode::Serialize(Serializer &serializer) {
	serializer.Write<QueryNodeType>(type);
	serializer.Write<bool>(select_distinct);
	serializer.WriteOptional(limit);
	serializer.WriteOptional(offset);
	serializer.Write<index_t>(orders.size());
	for (index_t i = 0; i < orders.size(); i++) {
		serializer.Write<OrderType>(orders[i].type);
		orders[i].expression->Serialize(serializer);
	}
}

unique_ptr<QueryNode> QueryNode::Deserialize(Deserializer &source) {
	unique_ptr<QueryNode> result;
	auto type = source.Read<QueryNodeType>();
	auto select_distinct = source.Read<bool>();
	auto limit = source.ReadOptional<ParsedExpression>();
	auto offset = source.ReadOptional<ParsedExpression>();
	index_t order_count = source.Read<index_t>();
	vector<OrderByNode> orders;
	for (index_t i = 0; i < order_count; i++) {
		OrderByNode node;
		node.type = source.Read<OrderType>();
		node.expression = ParsedExpression::Deserialize(source);
		orders.push_back(move(node));
	}
	switch (type) {
	case QueryNodeType::SELECT_NODE:
		result = SelectNode::Deserialize(source);
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		result = SetOperationNode::Deserialize(source);
		break;
	default:
		throw SerializationException("Could not deserialize Query Node: unknown type!");
	}
	result->select_distinct = select_distinct;
	result->limit = move(limit);
	result->offset = move(offset);
	result->orders = move(orders);
	return result;
}
