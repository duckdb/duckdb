#include "duckdb/parser/query_node/set_operation_node.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

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

} // namespace duckdb
