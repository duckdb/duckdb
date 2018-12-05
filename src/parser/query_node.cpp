#include "parser/query_node.hpp"

#include "parser/query_node/select_node.hpp"
#include "parser/query_node/set_operation_node.hpp"

using namespace duckdb;
using namespace std;

void QueryNode::Serialize(Serializer &serializer) {
	serializer.Write<QueryNodeType>(type);
	serializer.Write<bool>(select_distinct);
}

unique_ptr<QueryNode> QueryNode::Deserialize(Deserializer &source) {
	unique_ptr<QueryNode> result;
	auto type = source.Read<QueryNodeType>();
	auto select_distinct = source.Read<bool>();
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
	return result;
}
