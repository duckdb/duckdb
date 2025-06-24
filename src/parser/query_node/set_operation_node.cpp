#include "duckdb/parser/query_node/set_operation_node.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

SetOperationNode::SetOperationNode() : QueryNode(QueryNodeType::SET_OPERATION_NODE) {
}

string SetOperationNode::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "(" + left->ToString() + ") ";

	switch (setop_type) {
	case SetOperationType::UNION:
		result += setop_all ? "UNION ALL" : "UNION";
		break;
	case SetOperationType::UNION_BY_NAME:
		result += setop_all ? "UNION ALL BY NAME" : "UNION BY NAME";
		break;
	case SetOperationType::EXCEPT:
		result += setop_all ? "EXCEPT ALL" : "EXCEPT";
		break;
	case SetOperationType::INTERSECT:
		result += setop_all ? "INTERSECT ALL" : "INTERSECT";
		break;
	default:
		throw InternalException("Unsupported set operation type");
	}
	result += " (" + right->ToString() + ")";
	return result + ResultModifiersToString();
}

bool SetOperationNode::Equals(const QueryNode *other_p) const {
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	if (this == other_p) {
		return true;
	}
	auto &other = other_p->Cast<SetOperationNode>();
	if (setop_type != other.setop_type) {
		return false;
	}
	if (setop_all != other.setop_all) {
		return false;
	}
	if (!left->Equals(other.left.get())) {
		return false;
	}
	if (!right->Equals(other.right.get())) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> SetOperationNode::Copy() const {
	auto result = make_uniq<SetOperationNode>();
	result->setop_type = setop_type;
	result->setop_all = setop_all;
	result->left = left->Copy();
	result->right = right->Copy();
	this->CopyProperties(*result);
	return std::move(result);
}

SetOperationNode::SetOperationNode(SetOperationType setop_type, unique_ptr<QueryNode> left, unique_ptr<QueryNode> right,
                                   vector<unique_ptr<QueryNode>> children, bool setop_all)
    : QueryNode(QueryNodeType::SET_OPERATION_NODE), setop_type(setop_type), setop_all(setop_all) {
	if (left && right) {
		// simple case - left/right are supplied
		this->left = std::move(left);
		this->right = std::move(right);
		return;
	}
	if (children.size() == 2) {
		this->left = std::move(children[0]);
		this->right = std::move(children[1]);
	}
	// we have multiple children - we need to construct a tree of set operation nodes
	if (children.size() <= 1) {
		throw SerializationException("Set Operation requires at least 2 children");
	}
	if (setop_type != SetOperationType::UNION) {
		throw SerializationException("Multiple children in set-operations are only supported for UNION");
	}
	// construct a balanced tree from the union
	while (children.size() > 2) {
		vector<unique_ptr<QueryNode>> new_children;
		for (idx_t i = 0; i < children.size(); i += 2) {
			if (i + 1 == children.size()) {
				new_children.push_back(std::move(children[i]));
			} else {
				vector<unique_ptr<QueryNode>> empty_children;
				auto setop_node =
				    make_uniq<SetOperationNode>(setop_type, std::move(children[i]), std::move(children[i + 1]),
				                                std::move(empty_children), setop_all);
				new_children.push_back(std::move(setop_node));
			}
		}
		children = std::move(new_children);
	}
	// two children left - fill in the left/right of this node
	this->left = std::move(children[0]);
	this->right = std::move(children[1]);
}

vector<unique_ptr<QueryNode>> SetOperationNode::SerializeChildNodes() const {
	// we always serialize children as left/right currently
	return vector<unique_ptr<QueryNode>>();
}

} // namespace duckdb
