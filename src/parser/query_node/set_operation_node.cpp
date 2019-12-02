#include "duckdb/parser/query_node/set_operation_node.hpp"

using namespace duckdb;
using namespace std;

bool SetOperationNode::Equals(const QueryNode *other_) const {
	if (!QueryNode::Equals(other_)) {
		return false;
	}
	if (this == other_) {
		return true;
	}
	auto other = (SetOperationNode *)other_;
	if (setop_type != other->setop_type) {
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

unique_ptr<QueryNode> SetOperationNode::Copy() {
	auto result = make_unique<SetOperationNode>();
	result->setop_type = setop_type;
	result->left = left->Copy();
	result->right = right->Copy();
	this->CopyProperties(*result);
	return move(result);
}

void SetOperationNode::Serialize(Serializer &serializer) {
	QueryNode::Serialize(serializer);
	serializer.Write<SetOperationType>(setop_type);
	left->Serialize(serializer);
	right->Serialize(serializer);
}

unique_ptr<QueryNode> SetOperationNode::Deserialize(Deserializer &source) {
	auto result = make_unique<SetOperationNode>();
	result->setop_type = source.Read<SetOperationType>();
	result->left = QueryNode::Deserialize(source);
	result->right = QueryNode::Deserialize(source);
	return move(result);
}
