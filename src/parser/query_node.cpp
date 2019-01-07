#include "parser/query_node.hpp"

#include "parser/query_node/select_node.hpp"
#include "parser/query_node/set_operation_node.hpp"

using namespace duckdb;
using namespace std;

bool QueryNode::Equals(const QueryNode *other) const {
	if (!other) {
		return false;
	}
	if (select_distinct != other->select_distinct) {
		return false;
	}
	if (limit.limit != other->limit.limit || limit.offset != other->limit.offset) {
		return false;
	}
	if (limit.limit != other->limit.limit || limit.offset != other->limit.offset) {
		return false;
	}
	if (orderby.orders.size() != other->orderby.orders.size()) {
		return false;
	}
	for (size_t i = 0; i < orderby.orders.size(); i++) {
		if (orderby.orders[i].type != other->orderby.orders[i].type ||
		    !orderby.orders[i].expression->Equals(other->orderby.orders[i].expression.get())) {
			return false;
		}
	}
	return other->type == type;
}

void QueryNode::CopyProperties(QueryNode &other) {
	other.select_distinct = select_distinct;
	// order
	for (auto &order : orderby.orders) {
		other.orderby.orders.push_back(OrderByNode(order.type, order.expression->Copy()));
	}
	// limit
	other.limit.limit = limit.limit;
	other.limit.offset = limit.offset;
}

void QueryNode::Serialize(Serializer &serializer) {
	serializer.Write<QueryNodeType>(type);
	serializer.Write<bool>(select_distinct);
	serializer.Write<int64_t>(limit.limit);
	serializer.Write<int64_t>(limit.offset);
	serializer.Write<int64_t>(orderby.orders.size());
	for (size_t i = 0; i < orderby.orders.size(); i++) {
		serializer.Write<OrderType>(orderby.orders[i].type);
		orderby.orders[i].expression->Serialize(serializer);
	}
}

unique_ptr<QueryNode> QueryNode::Deserialize(Deserializer &source) {
	unique_ptr<QueryNode> result;
	auto type = source.Read<QueryNodeType>();
	auto select_distinct = source.Read<bool>();
	LimitDescription limit;
	limit.limit = source.Read<int64_t>();
	limit.offset = source.Read<int64_t>();
	auto order_count = source.Read<int64_t>();
	OrderByDescription orderby;
	for (size_t i = 0; i < order_count; i++) {
		OrderByNode node;
		node.type = source.Read<OrderType>();
		node.expression = Expression::Deserialize(source);
		orderby.orders.push_back(move(node));
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
	result->limit = limit;
	result->orderby = move(orderby);
	return result;
}
