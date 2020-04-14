#include "duckdb/parser/query_node.hpp"

#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"

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
	if (modifiers.size() != other->modifiers.size()) {
		return false;
	}
	for (idx_t i = 0; i < modifiers.size(); i++) {
		if (!modifiers[i]->Equals(other->modifiers[i].get())) {
			return false;
		}
	}
	return other->type == type;
}

void QueryNode::CopyProperties(QueryNode &other) {
	for (auto &modifier : modifiers) {
		other.modifiers.push_back(modifier->Copy());
	}
}

void QueryNode::Serialize(Serializer &serializer) {
	serializer.Write<QueryNodeType>(type);
	serializer.Write<idx_t>(modifiers.size());
	for (idx_t i = 0; i < modifiers.size(); i++) {
		modifiers[i]->Serialize(serializer);
	}
}

unique_ptr<QueryNode> QueryNode::Deserialize(Deserializer &source) {
	unique_ptr<QueryNode> result;
	auto type = source.Read<QueryNodeType>();
	auto modifier_count = source.Read<idx_t>();
	vector<unique_ptr<ResultModifier>> modifiers;
	for (idx_t i = 0; i < modifier_count; i++) {
		modifiers.push_back(ResultModifier::Deserialize(source));
	}
	switch (type) {
	case QueryNodeType::SELECT_NODE:
		result = SelectNode::Deserialize(source);
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		result = SetOperationNode::Deserialize(source);
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		result = RecursiveCTENode::Deserialize(source);
		break;
	default:
		throw SerializationException("Could not deserialize Query Node: unknown type!");
	}
	result->modifiers = move(modifiers);
	return result;
}
