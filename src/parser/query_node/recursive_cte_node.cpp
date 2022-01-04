#include "duckdb/parser/query_node/recursive_cte_node.hpp"

namespace duckdb {

bool RecursiveCTENode::Equals(const QueryNode *other_p) const {
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	if (this == other_p) {
		return true;
	}
	auto other = (RecursiveCTENode *)other_p;

	if (other->union_all != union_all) {
		return false;
	}
	if (!left->Equals(other->left.get())) {
		return false;
	}
	if (!right->Equals(other->right.get())) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> RecursiveCTENode::Copy() {
	auto result = make_unique<RecursiveCTENode>();
	result->ctename = ctename;
	result->union_all = union_all;
	result->left = left->Copy();
	result->right = right->Copy();
	result->aliases = aliases;
	this->CopyProperties(*result);
	return result;
}

void RecursiveCTENode::Serialize(Serializer &serializer) {
	QueryNode::Serialize(serializer);
	serializer.WriteString(ctename);
	serializer.WriteString(union_all ? "T" : "F");
	left->Serialize(serializer);
	right->Serialize(serializer);
	serializer.WriteStringVector(aliases);
}

unique_ptr<QueryNode> RecursiveCTENode::Deserialize(Deserializer &source) {
	auto result = make_unique<RecursiveCTENode>();
	result->ctename = source.Read<string>();
	result->union_all = source.Read<string>() == "T" ? true : false;
	result->left = QueryNode::Deserialize(source);
	result->right = QueryNode::Deserialize(source);
	source.ReadStringVector(result->aliases);
	return result;
}

} // namespace duckdb
