#include "duckdb/parser/query_node/recursive_cte_node.hpp"

using namespace duckdb;
using namespace std;

bool RecursiveCTENode::Equals(const QueryNode *other_) const {
	if (!QueryNode::Equals(other_)) {
		return false;
	}
	if (this == other_) {
		return true;
	}
	auto other = (RecursiveCTENode *)other_;

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
	this->CopyProperties(*result);
	return move(result);
}

void RecursiveCTENode::Serialize(Serializer &serializer) {
	QueryNode::Serialize(serializer);
	serializer.WriteString(ctename);
	serializer.WriteString(union_all ? "T" : "F");
	left->Serialize(serializer);
	right->Serialize(serializer);
}

unique_ptr<QueryNode> RecursiveCTENode::Deserialize(Deserializer &source) {
	auto result = make_unique<RecursiveCTENode>();
	result->ctename = source.Read<string>();
	result->union_all = source.Read<string>() == "T" ? true : false;
	result->left = QueryNode::Deserialize(source);
	result->right = QueryNode::Deserialize(source);
	return move(result);
}
