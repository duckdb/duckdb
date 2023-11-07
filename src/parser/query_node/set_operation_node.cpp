#include "duckdb/parser/query_node/set_operation_node.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

string SetOperationNode::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "(" + left->ToString() + ") ";
	bool is_distinct = false;
	for (idx_t modifier_idx = 0; modifier_idx < modifiers.size(); modifier_idx++) {
		if (modifiers[modifier_idx]->type == ResultModifierType::DISTINCT_MODIFIER) {
			is_distinct = true;
			break;
		}
	}

	switch (setop_type) {
	case SetOperationType::UNION:
		result += is_distinct ? "UNION" : "UNION ALL";
		break;
	case SetOperationType::UNION_BY_NAME:
		result += is_distinct ? "UNION BY NAME" : "UNION ALL BY NAME";
		break;
	case SetOperationType::EXCEPT:
		D_ASSERT(is_distinct);
		result += "EXCEPT";
		break;
	case SetOperationType::INTERSECT:
		D_ASSERT(is_distinct);
		result += "INTERSECT";
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
	result->left = left->Copy();
	result->right = right->Copy();
	this->CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
