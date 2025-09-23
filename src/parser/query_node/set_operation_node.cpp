#include "duckdb/parser/query_node/set_operation_node.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

SetOperationNode::SetOperationNode() : QueryNode(QueryNodeType::SET_OPERATION_NODE) {
}

const vector<unique_ptr<ParsedExpression>> &SetOperationNode::GetSelectList() const {
	return children[0]->GetSelectList();
}

string SetOperationNode::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "(" + children[0]->ToString() + ") ";

	for (idx_t i = 1; i < children.size(); i++) {
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
		result += " (" + children[i]->ToString() + ")";
	}
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
	if (children.size() != other.children.size()) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!children[i]->Equals(other.children[i].get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<QueryNode> SetOperationNode::Copy() const {
	auto result = make_uniq<SetOperationNode>();
	result->setop_type = setop_type;
	result->setop_all = setop_all;
	for (auto &child : children) {
		result->children.push_back(child->Copy());
	}
	this->CopyProperties(*result);
	return std::move(result);
}

SetOperationNode::SetOperationNode(SetOperationType setop_type, unique_ptr<QueryNode> left, unique_ptr<QueryNode> right,
                                   vector<unique_ptr<QueryNode>> children_p, bool setop_all)
    : QueryNode(QueryNodeType::SET_OPERATION_NODE), setop_type(setop_type), setop_all(setop_all) {
	if (children_p.empty()) {
		if (!left || !right) {
			throw SerializationException("Error deserializing SetOperationNode - left/right or children must be set");
		}
		children.push_back(std::move(left));
		children.push_back(std::move(right));
	} else {
		if (left || right) {
			throw SerializationException("Error deserializing SetOperationNode - left/right or children must be set");
		}
		children = std::move(children_p);
	}
	if (children.size() < 2) {
		throw SerializationException("SetOperationNode must have at least two children");
	}
}

unique_ptr<QueryNode> SetOperationNode::SerializeChildNode(idx_t index) const {
	// backwards compatibility - serialize as left/right if we have exactly two children
	if (children.size() == 2) {
		return children[index]->Copy();
	}
	return nullptr;
}

vector<unique_ptr<QueryNode>> SetOperationNode::SerializeChildNodes() const {
	// backwards compatibility - only serialize child list if we have more than two children
	if (children.size() != 2) {
		vector<unique_ptr<QueryNode>> nodes;
		for (auto &child : children) {
			nodes.push_back(child->Copy());
		}
		return nodes;
	}
	return vector<unique_ptr<QueryNode>>();
}

} // namespace duckdb
